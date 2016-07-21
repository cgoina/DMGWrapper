package dmg

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
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

// Attrs registers DMG client and server attributes
type Attrs struct {
	Configs          arg.StringList
	helpFlag         bool
	serverAddress    string
	serverPort       int
	nSections        int
	iterations       int
	vCycles          int
	iWeight          float64
	gWeight          float64
	gScale           float64
	nThreads         int
	verbose          bool
	gray             bool
	deramp           bool
	tileExt          string
	tileWidth        int
	tileHeight       int
	clientIndex      int
	sourcePixelsList arg.StringList
	sourceLabelsList arg.StringList
	sourcePixels     string
	sourceLabels     string
	destImg          string
	scratchDir       string
}

// Name method
func (a *Attrs) Name() string {
	return "dmg"
}

// DefineArgs method
func (a *Attrs) DefineArgs(fs *flag.FlagSet) {
	fs.Var(&a.Configs, "config", "list of configuration files which applied in the order they are specified")
	fs.IntVar(&a.nSections, "sections", 1, "Number of sections processed in parallel")
	fs.IntVar(&a.iterations, "iters", 5, "Number of Gauss-Siebel iterations")
	fs.IntVar(&a.vCycles, "vCycles", 1, "Number of V-cycles")
	fs.Float64Var(&a.iWeight, "iWeight", 0, "Value interpolation weight")
	fs.Float64Var(&a.gWeight, "gWeight", 1, "Gradient interpolation weight")
	fs.Float64Var(&a.gScale, "gScale", 1, "Gradient scale")
	fs.StringVar(&a.serverAddress, "serverAddress", "localhost", "DMG server address - host[:port]")
	fs.IntVar(&a.serverPort, "serverPort", 0, "DMG server port")
	fs.BoolVar(&a.verbose, "verbose", false, "verbosity flag")
	fs.BoolVar(&a.gray, "gray", true, "gray image flag")
	fs.BoolVar(&a.deramp, "deramp", true, "deramp flag")
	fs.IntVar(&a.tileWidth, "tileWidth", 8192, "Tile width")
	fs.IntVar(&a.tileHeight, "tileHeight", 8192, "Tile height")
	fs.StringVar(&a.tileExt, "tileExt", "png", "Destination image extension")
	fs.BoolVar(&a.helpFlag, "h", false, "gray image flag")
	fs.IntVar(&a.clientIndex, "clientIndex", 0, "Client index")
	fs.IntVar(&a.nThreads, "threads", 1, "Number of threads")
	fs.Var(&a.sourcePixelsList, "pixelsList", "List of image pixels")
	fs.Var(&a.sourceLabelsList, "labelsList", "List of image labels")
	fs.StringVar(&a.sourcePixels, "pixels", "", "Source image pixels")
	fs.StringVar(&a.sourceLabels, "labels", "", "Source image labels")
	fs.StringVar(&a.destImg, "out", "", "Destination image")
	fs.StringVar(&a.scratchDir, "temp", "/var/tmp", "Scratch directory")
}

// IsHelpFlagSet method
func (a *Attrs) IsHelpFlagSet() bool {
	return a.helpFlag
}

// validate arguments
func (a *Attrs) validate() error {
	nImages := len(a.sourcePixelsList)
	if len(a.sourceLabelsList) != nImages {
		return fmt.Errorf("PixelsList and LabelsList must have the same length")
	}
	if nImages == 0 {
		if a.sourcePixels == "" {
			return fmt.Errorf("No source pixels has been defined")
		}
		if a.sourceLabels == "" {
			return fmt.Errorf("No source labels has been defined")
		}
		if a.nSections > 1 {
			return fmt.Errorf("The number of sections must be equal to the number of source images")
		}
		return nil
	}
	if nImages != a.nSections {
		return fmt.Errorf("The number of sections must be equal to the number of source images")
	}
	for i := 0; i < nImages; i++ {
		sourcePixels := a.sourcePixelsList[i]
		sourceLabels := a.sourceLabelsList[i]
		if sourcePixels == "" {
			return fmt.Errorf("Pixels image not defined at index %d", i)
		}
		if sourceLabels == "" {
			return fmt.Errorf("Labels image not defined at index %d", i)
		}
	}
	return nil
}

func (a *Attrs) extractDmgAttrs(ja *arg.Args) (err error) {
	if a.serverAddress, err = ja.GetStringArgValue("serverAddress"); err != nil {
		return err
	}
	if a.serverPort, err = ja.GetIntArgValue("serverPort"); err != nil {
		return err
	}
	if a.nSections, err = ja.GetIntArgValue("sections"); err != nil {
		return err
	}
	if a.iterations, err = ja.GetIntArgValue("iters"); err != nil {
		return err
	}
	if a.vCycles, err = ja.GetIntArgValue("vCycles"); err != nil {
		return err
	}
	if a.iWeight, err = ja.GetFloat64ArgValue("iWeight"); err != nil {
		return err
	}
	if a.gWeight, err = ja.GetFloat64ArgValue("gWeight"); err != nil {
		return err
	}
	if a.gScale, err = ja.GetFloat64ArgValue("gScale"); err != nil {
		return err
	}
	if a.verbose, err = ja.GetBoolArgValue("verbose"); err != nil {
		return err
	}
	if a.gray, err = ja.GetBoolArgValue("gray"); err != nil {
		return err
	}
	if a.deramp, err = ja.GetBoolArgValue("deramp"); err != nil {
		return err
	}
	if a.tileWidth, err = ja.GetIntArgValue("tileWidth"); err != nil {
		return err
	}
	if a.tileHeight, err = ja.GetIntArgValue("tileHeight"); err != nil {
		return err
	}
	if a.tileExt, err = ja.GetStringArgValue("tileExt"); err != nil {
		return err
	}
	if a.clientIndex, err = ja.GetIntArgValue("clientIndex"); err != nil {
		return err
	}
	if a.nThreads, err = ja.GetIntArgValue("threads"); err != nil {
		return err
	}
	if a.sourcePixels, err = ja.GetStringArgValue("pixels"); err != nil {
		return err
	}
	if a.sourceLabels, err = ja.GetStringArgValue("labels"); err != nil {
		return err
	}
	if a.destImg, err = ja.GetStringArgValue("out"); err != nil {
		return err
	}
	if a.sourcePixelsList, err = ja.GetStringListArgValue("pixelsList"); err != nil {
		return err
	}
	if a.sourceLabelsList, err = ja.GetStringListArgValue("labelsList"); err != nil {
		return err
	}
	if a.scratchDir, err = ja.GetStringArgValue("temp"); err != nil {
		return err
	}
	return nil
}

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
	return lci.cmd.Wait()
}

// LocalDmgProcessor - in charge with starting a DMG Server process
type LocalDmgProcessor struct {
	Resources config.Config
}

// Process launches the server
func (ls LocalDmgProcessor) Process(j process.Job) (process.Info, error) {
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

// DMGService orchestrates the DMG client and server
type DMGService struct {
	DMGProcessor process.Processor
	Resources    config.Config
}

// ProcessDMG
func (s DMGService) ProcessDMG(args *arg.Args) error {
	var err error
	var dmgAttrs Attrs

	if err = dmgAttrs.extractDmgAttrs(args); err != nil {
		return err
	}
	if err = dmgAttrs.validate(); err != nil {
		return err
	}
	serverArgs := *args
	serverJob := process.Job{
		Executable:     s.Resources.GetStringProperty("dmgServer"),
		JArgs:          serverArgs,
		CmdlineBuilder: serverCmdlineBuilder{},
	}
	serverJobInfo, serverAddress, err := s.startDMGServer(serverJob)
	if err != nil {
		return err
	}
	go func() {
		if waitErr := serverJobInfo.WaitForTermination(); waitErr != nil {
			log.Printf("Error waiting for the DMG Server to terminate: %v", waitErr)
		}
	}()

	clientArgs := *args
	clientArgs.UpdateStringArg("serverAddress", serverAddress)
	clientJob := process.Job{
		Executable:     s.Resources.GetStringProperty("dmgClient"),
		JArgs:          clientArgs,
		CmdlineBuilder: clientCmdlineBuilder{},
	}
	log.Printf("Start DMG Client")
	clientJobInfo, err := s.DMGProcessor.Process(clientJob)
	if err != nil {
		log.Fatalf("Error starting for the DMG Client: %v", err)
	}
	if err = clientJobInfo.WaitForTermination(); err != nil {
		log.Fatalf("Error waiting for the DMG Client to terminate")
	}
	return nil
}

func (s DMGService) startDMGServer(j process.Job) (process.Info, string, error) {
	log.Printf("Start DMG Server")
	jobInfo, err := s.DMGProcessor.Process(j)
	if err != nil {
		return jobInfo, "", err
	}
	jobOutput, err := jobInfo.JobStdout()
	if err != nil {
		return jobInfo, "", err
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

// imageBandSplitter - splits the based on the number of image bands.
// The number of entries in the pixels list must be equal to the number of entries in the labels list.
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
		newJob, err := s.createJob(&j, 0, dmgAttrs.sourcePixels, dmgAttrs.sourceLabels)
		if err != nil {
			return err
		}
		jch <- *newJob
		return nil
	}
	for i := 0; i < nImages; i++ {
		newJob, err := s.createJob(&j, 0, dmgAttrs.sourcePixelsList[i], dmgAttrs.sourceLabelsList[i])
		if err != nil {
			return err
		}
		jch <- *newJob
	}
	return nil
}

func (s imageBandSplitter) createJob(originalJob *process.Job,
	jobIndex int,
	sourcePixels, sourceLabels string) (*process.Job, error) {
	if sourcePixels == "" {
		return nil, fmt.Errorf("No source pixels has been defined")
	}
	if sourceLabels == "" {
		return nil, fmt.Errorf("No source labels has been defined")
	}
	newJobArgs := originalJob.JArgs
	newJobArgs.UpdateStringArg("pixels", sourcePixels)
	newJobArgs.UpdateStringArg("labels", sourceLabels)
	return &process.Job{
		Executable:     originalJob.Executable,
		Name:           fmt.Sprintf("%s_%d", originalJob.Name, jobIndex),
		JArgs:          newJobArgs,
		CmdlineBuilder: originalJob.CmdlineBuilder,
	}, nil
}
