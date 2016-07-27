package dmg

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"arg"
	"config"
	"process"
)

const (
	pauseBetweenChecksInSec = 10
	maxChecks               = 100
	serverAddressPrefix     = "Server Address: "
)

// localCmdInfo local process info
type localCmdInfo struct {
	cmd       *exec.Cmd
	jobStdout io.ReadCloser
	jobStderr io.ReadCloser
}

func (lci *localCmdInfo) JobStdout() (io.ReadCloser, error) {
	return lci.jobStdout, nil
}

func (lci *localCmdInfo) JobStderr() (io.ReadCloser, error) {
	return lci.jobStderr, nil
}

func (lci *localCmdInfo) WaitForTermination() error {
	var donech chan struct{}
	var done struct{}
	donech = make(chan struct{})
	go func() {
		for {
			select {
			case <-time.After(500 * time.Millisecond):
				lci.readOutput()
			case <-donech:
				lci.readOutput()
				return
			}
		}
	}()
	lci.readOutput()
	err := lci.cmd.Wait()
	donech <- done
	return err
}

func (lci *localCmdInfo) readOutput() {
	io.Copy(os.Stdout, lci.jobStdout)
	io.Copy(os.Stderr, lci.jobStderr)
}

// LocalDmgProcessor - in charge with starting a DMG Server process
type LocalDmgProcessor struct {
	process.JobWatcher
	Resources config.Config
}

// Run the given job
func (lp LocalDmgProcessor) Run(j process.Job) error {
	ji, err := lp.Start(j)
	if err != nil {
		return fmt.Errorf("Error starting %v: %v", j, err)
	}
	return lp.Wait(ji)
}

// Start launches the server
func (lp LocalDmgProcessor) Start(j process.Job) (process.Info, error) {
	return processJob(j)
}

// serverCmdlineBuilder - DMG server command line builder
type serverCmdlineBuilder struct {
}

// GetCmdlineArgs server command line builder method
func (sclb serverCmdlineBuilder) GetCmdlineArgs(a arg.Args) ([]string, error) {
	var cmdargs []string
	var err error
	var dmgAttrs Attrs

	if err = dmgAttrs.extractDmgAttrs(&a); err != nil {
		return cmdargs, err
	}
	if dmgAttrs.serverPort > 0 {
		cmdargs = arg.AddArgs(cmdargs, "--port", strconv.FormatInt(int64(dmgAttrs.serverPort), 10))
	}
	cmdargs = arg.AddArgs(cmdargs, "--count", strconv.FormatInt(int64(dmgAttrs.nSections), 10))
	cmdargs = arg.AddArgs(cmdargs, "--iters", strconv.FormatInt(int64(dmgAttrs.iterations), 10))
	cmdargs = arg.AddArgs(cmdargs, "--vCycles", strconv.FormatInt(int64(dmgAttrs.vCycles), 10))
	cmdargs = arg.AddArgs(cmdargs, "--iWeight", strconv.FormatFloat(dmgAttrs.iWeight, 'g', -1, 64))
	cmdargs = arg.AddArgs(cmdargs, "--gWeight", strconv.FormatFloat(dmgAttrs.gWeight, 'g', -1, 64))
	cmdargs = arg.AddArgs(cmdargs, "--gScale", strconv.FormatFloat(dmgAttrs.gScale, 'g', -1, 64))
	cmdargs = arg.AddArgs(cmdargs, "--tileExt", dmgAttrs.tileExt)
	cmdargs = arg.AddArgs(cmdargs, "--tileWidth", strconv.FormatInt(int64(dmgAttrs.tileWidth), 10))
	cmdargs = arg.AddArgs(cmdargs, "--tileHeight", strconv.FormatInt(int64(dmgAttrs.tileHeight), 10))

	if dmgAttrs.verbose {
		cmdargs = arg.AddArgs(cmdargs, "--verbose")
	}
	if dmgAttrs.gray {
		cmdargs = arg.AddArgs(cmdargs, "--gray")
	}
	if dmgAttrs.deramp {
		cmdargs = arg.AddArgs(cmdargs, "--deramp")
	}
	return cmdargs, nil
}

// clientCmdlineBuilder - DMG client command line builder
type clientCmdlineBuilder struct {
}

// GetCmdlineArgs client command line builder method
func (sclb clientCmdlineBuilder) GetCmdlineArgs(a arg.Args) ([]string, error) {
	var cmdargs []string
	var err error
	var dmgAttrs Attrs

	if err = dmgAttrs.extractDmgAttrs(&a); err != nil {
		return cmdargs, err
	}
	if dmgAttrs.serverPort > 0 {
		cmdargs = arg.AddArgs(cmdargs, "--port", strconv.FormatInt(int64(dmgAttrs.serverPort), 10))
	}
	if dmgAttrs.serverAddress != "" {
		cmdargs = arg.AddArgs(cmdargs, "--address", dmgAttrs.serverAddress)
	}
	if dmgAttrs.clientIndex > 0 {
		cmdargs = arg.AddArgs(cmdargs, "--index", strconv.FormatInt(int64(dmgAttrs.clientIndex), 10))
	}
	if dmgAttrs.nThreads > 1 {
		cmdargs = arg.AddArgs(cmdargs, "--threads", strconv.FormatInt(int64(dmgAttrs.nThreads), 10))
	}
	cmdargs = arg.AddArgs(cmdargs, "--pixels", dmgAttrs.sourcePixels)
	cmdargs = arg.AddArgs(cmdargs, "--labels", dmgAttrs.sourceLabels)
	cmdargs = arg.AddArgs(cmdargs, "--out", dmgAttrs.destImg)
	cmdargs = arg.AddArgs(cmdargs, "--temp", dmgAttrs.scratchDir)
	return cmdargs, nil
}

func processJob(j process.Job) (process.Info, error) {
	cmdargs, err := j.CmdlineBuilder.GetCmdlineArgs(j.JArgs)
	if err != nil {
		return nil, fmt.Errorf("Error preparing the command line arguments: %v", err)
	}
	cmd := exec.Command(j.Executable, cmdargs...)
	log.Printf("Execute %v\n", cmd)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("Error opening the command stdout: %v", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("Error opening the command stderr: %v", err)
	}
	lci := &localCmdInfo{
		cmd:       cmd,
		jobStdout: stdout,
		jobStderr: stderr,
	}
	err = cmd.Start()
	return lci, err
}

// imageBandsProcessingInfo job info related to processing
type imageBandsProcessingInfo struct {
	serverJobInfo process.Info
	clientJobInfo process.Info
}

// JobStdout job's standard output
func (pi imageBandsProcessingInfo) JobStdout() (io.ReadCloser, error) {
	return os.Stdout, nil
}

// JobStderr job's standard error
func (pi imageBandsProcessingInfo) JobStderr() (io.ReadCloser, error) {
	return os.Stderr, nil
}

// WaitForTermination wait for job's completion
func (pi imageBandsProcessingInfo) WaitForTermination() error {
	go func() {
		if pi.serverJobInfo == nil {
			log.Printf("No server job has been started")
			return
		}
		if waitErr := pi.serverJobInfo.WaitForTermination(); waitErr != nil {
			log.Printf("Error waiting for the DMG Server to terminate: %v", waitErr)
		}
	}()
	if pi.clientJobInfo == nil {
		return fmt.Errorf("No client job has been started")
	}
	if err := pi.clientJobInfo.WaitForTermination(); err != nil {
		return fmt.Errorf("Error waiting for the DMG Client to terminate")
	}
	log.Printf("DMG processing completed")
	return nil
}

// ImageBandsProcessor orchestrates the DMG client and server for one or multiple images
type ImageBandsProcessor struct {
	process.JobWatcher
	ImageProcessor process.Processor
	Resources      config.Config
}

// Run the given job
func (p ImageBandsProcessor) Run(j process.Job) error {
	ji, err := p.Start(j)
	if err != nil {
		return fmt.Errorf("Error starting %v: %v", j, err)
	}
	return p.Wait(ji)
}

// Start the distributed gradient processing
func (p ImageBandsProcessor) Start(j process.Job) (process.Info, error) {
	var err error
	var dmgAttrs Attrs

	args := &j.JArgs
	processInfo := imageBandsProcessingInfo{}
	if err = dmgAttrs.extractDmgAttrs(args); err != nil {
		return processInfo, err
	}
	if err = dmgAttrs.validate(); err != nil {
		return processInfo, err
	}
	serverArgs := args.Clone()
	serverJob := process.Job{
		Name:           fmt.Sprintf("%s-Server", j.Name),
		Executable:     p.Resources.GetStringProperty("dmgServer"),
		JArgs:          serverArgs,
		CmdlineBuilder: serverCmdlineBuilder{},
	}
	serverJobInfo, serverAddress, err := p.startDMGServer(serverJob)
	if err != nil {
		return processInfo, err
	}
	processInfo.serverJobInfo = serverJobInfo

	clientArgs := args.Clone()
	clientArgs.UpdateStringArg("serverAddress", serverAddress)
	clientJob := process.Job{
		Name:           fmt.Sprintf("%s-Client", j.Name),
		Executable:     p.Resources.GetStringProperty("dmgClient"),
		JArgs:          clientArgs,
		CmdlineBuilder: clientCmdlineBuilder{},
	}
	var clientJobSplitter imageBandSplitter
	clientProcessor := process.NewParallelProcessor(p.ImageProcessor, clientJobSplitter, p.Resources)

	log.Printf("Start DMG Client")
	clientJobInfo, err := clientProcessor.Start(clientJob)
	if err != nil {
		return processInfo, fmt.Errorf("Error starting for the DMG Client: %v", err)
	}
	processInfo.clientJobInfo = clientJobInfo
	return processInfo, nil
}

func (p ImageBandsProcessor) startDMGServer(j process.Job) (process.Info, string, error) {
	log.Printf("Start DMG Server")
	jobInfo, err := p.ImageProcessor.Start(j)
	if err != nil {
		return jobInfo, "", err
	}
	jobOutput, err := jobInfo.JobStdout()
	if err != nil {
		return jobInfo, "", err
	}
	serverAddress, err := j.JArgs.GetStringArgValue("serverAddress")
	if err != nil {
		log.Printf("Error getting the serverAddress: %v", err)
	}
	if serverAddress != "" {
		return jobInfo, serverAddress, nil
	}
	r := bufio.NewReader(jobOutput)
	for i := 0; i < maxChecks; i++ {
		line, err := r.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				// there was no new output since the last read
				time.Sleep(pauseBetweenChecksInSec * time.Second)
				continue
			}
			// there was some other error than EOF so stop here
			return jobInfo, "", err
		}
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, serverAddressPrefix) {
			serverAddress := strings.TrimLeft(line, serverAddressPrefix)
			log.Printf("Server started on %s:", serverAddress)
			return jobInfo, serverAddress, nil
		}
		time.Sleep(pauseBetweenChecksInSec * time.Second)
	}
	return jobInfo, "", fmt.Errorf("Timeout - could not read server's address")
}

// imageBandSplitter - splits the job based on the number of entries in the pixels and labels list.
// Each entry typically corresponds to an image band and all bands are solved on the same server instance.
// The number of entries in the pixels list must be equal to the number of entries in the labels list and
// there is a one to one correspondence between the two lists.
type imageBandSplitter struct {
}

// SplitJob splits the job into multiple parallelizable jobs
func (s imageBandSplitter) SplitJob(j process.Job, jch chan<- process.Job) error {
	var err error
	var dmgAttrs Attrs

	if err = dmgAttrs.extractDmgAttrs(&j.JArgs); err != nil {
		return err
	}
	nImages := len(dmgAttrs.sourcePixelsList)
	if nImages == 0 {
		newJob, err := s.createJob(j, 0,
			dmgAttrs.sourcePixels, dmgAttrs.sourceLabels, dmgAttrs.destImg)
		if err != nil {
			return err
		}
		jch <- newJob
		return nil
	}
	for i := 0; i < nImages; i++ {
		newJob, err := s.createJob(j, i,
			dmgAttrs.sourcePixelsList[i], dmgAttrs.sourceLabelsList[i], dmgAttrs.destImgList[i])
		if err != nil {
			return err
		}
		jch <- newJob
	}
	return nil
}

func (s imageBandSplitter) createJob(j process.Job, jobIndex int, pixels, labels, outputImg string) (process.Job, error) {
	if pixels == "" {
		return j, fmt.Errorf("No source pixels has been defined")
	}
	if labels == "" {
		return j, fmt.Errorf("No source labels has been defined")
	}
	if outputImg == "" {
		return j, fmt.Errorf("No output image has been defined")
	}
	newJobArgs := j.JArgs.Clone()
	newJobArgs.UpdateIntArg("clientIndex", jobIndex)
	newJobArgs.UpdateStringArg("pixels", pixels)
	newJobArgs.UpdateStringArg("labels", labels)
	newJobArgs.UpdateStringArg("out", outputImg)
	return process.Job{
		Executable:     j.Executable,
		Name:           fmt.Sprintf("%s_%d", j.Name, jobIndex),
		JArgs:          newJobArgs,
		CmdlineBuilder: j.CmdlineBuilder,
	}, nil
}
