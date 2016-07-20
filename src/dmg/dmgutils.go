package dmg

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os/exec"
	"strconv"

	"config"
	"job"
)

// Attrs registers DMG client and server attributes
type Attrs struct {
	Configs         job.ValueList
	helpFlag        bool
	serverAddress   string
	serverPort      int
	nSections       int
	iterations      int
	vCycles         int
	iWeight         float64
	gWeight         float64
	gScale          float64
	nThreads        int
	verbose         bool
	gray            bool
	deramp          bool
	tileExt         string
	tileWidth       int
	tileHeight      int
	clientIndex     int
	sourceImgPixels string
	sourceImgLabels string
	destImg         string
	scratchDir      string
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
	fs.StringVar(&a.sourceImgPixels, "pixels", "", "Source pixels image")
	fs.StringVar(&a.sourceImgLabels, "labels", "", "Source labels")
	fs.StringVar(&a.destImg, "out", "", "Destination image")
	fs.StringVar(&a.scratchDir, "scratchdir", "/var/tmp", "Scratch directory")
}

// IsHelpFlagSet method
func (a *Attrs) IsHelpFlagSet() bool {
	return a.helpFlag
}

func (a *Attrs) extractDmgAttrs(ja job.Args) (err error) {
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
	if a.sourceImgPixels, err = ja.GetStringArgValue("pixels"); err != nil {
		return err
	}
	if a.sourceImgLabels, err = ja.GetStringArgValue("labels"); err != nil {
		return err
	}
	if a.destImg, err = ja.GetStringArgValue("out"); err != nil {
		return err
	}
	if a.scratchDir, err = ja.GetStringArgValue("scratchdir"); err != nil {
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

// LocalDmgServer - in charge with starting a DMG Server process
type LocalDmgServer struct {
	Resources config.Config
}

// Process launches the server
func (ls LocalDmgServer) Process(j job.Job) (job.Info, error) {
	return processJob(j)
}

// ServerCmdlineBuilder - DMG server command line builder
type ServerCmdlineBuilder struct {
}

// GetCmdlineArgs server command line builder method
func (sclb ServerCmdlineBuilder) GetCmdlineArgs(a job.Args) ([]string, error) {
	var cmdargs []string
	var err error
	var dmgAttrs Attrs

	if err = dmgAttrs.extractDmgAttrs(a); err != nil {
		return cmdargs, err
	}
	if dmgAttrs.serverPort > 0 {
		cmdargs = job.AddArgs(cmdargs, "--port", strconv.FormatInt(int64(dmgAttrs.serverPort), 10))
	}
	cmdargs = job.AddArgs(cmdargs, "--count", strconv.FormatInt(int64(dmgAttrs.nSections), 10))
	cmdargs = job.AddArgs(cmdargs, "--iters", strconv.FormatInt(int64(dmgAttrs.iterations), 10))
	cmdargs = job.AddArgs(cmdargs, "--vCycles", strconv.FormatInt(int64(dmgAttrs.vCycles), 10))
	cmdargs = job.AddArgs(cmdargs, "--iWeight", strconv.FormatFloat(dmgAttrs.iWeight, 'g', -1, 64))
	cmdargs = job.AddArgs(cmdargs, "--gWeight", strconv.FormatFloat(dmgAttrs.gWeight, 'g', -1, 64))
	cmdargs = job.AddArgs(cmdargs, "--gScale", strconv.FormatFloat(dmgAttrs.gScale, 'g', -1, 64))
	cmdargs = job.AddArgs(cmdargs, "--tileExt", dmgAttrs.tileExt)
	cmdargs = job.AddArgs(cmdargs, "--tileWidth", strconv.FormatInt(int64(dmgAttrs.tileWidth), 10))
	cmdargs = job.AddArgs(cmdargs, "--tileHeight", strconv.FormatInt(int64(dmgAttrs.tileHeight), 10))

	if dmgAttrs.verbose {
		cmdargs = job.AddArgs(cmdargs, "--verbose")
	}
	if dmgAttrs.gray {
		cmdargs = job.AddArgs(cmdargs, "--gray")
	}
	if dmgAttrs.deramp {
		cmdargs = job.AddArgs(cmdargs, "--deramp")
	}
	return cmdargs, nil
}

// LocalDmgClient - in charge with starting a DMG Client process
type LocalDmgClient struct {
	Resources config.Config
}

// Process launches the client
func (ls LocalDmgClient) Process(j job.Job) (job.Info, error) {
	return processJob(j)
}

// ClientCmdlineBuilder - DMG client command line builder
type ClientCmdlineBuilder struct {
}

// GetCmdlineArgs client command line builder method
func (sclb ClientCmdlineBuilder) GetCmdlineArgs(a job.Args) ([]string, error) {
	var cmdargs []string
	var err error
	var dmgAttrs Attrs

	if err = dmgAttrs.extractDmgAttrs(a); err != nil {
		return cmdargs, err
	}
	if dmgAttrs.serverPort > 0 {
		cmdargs = job.AddArgs(cmdargs, "--port", strconv.FormatInt(int64(dmgAttrs.serverPort), 10))
	}
	if dmgAttrs.serverAddress != "" {
		cmdargs = job.AddArgs(cmdargs, "--address", dmgAttrs.serverAddress)
	}
	if dmgAttrs.clientIndex > 0 {
		cmdargs = job.AddArgs(cmdargs, "--index", strconv.FormatInt(int64(dmgAttrs.clientIndex), 10))
	}
	if dmgAttrs.nThreads > 1 {
		cmdargs = job.AddArgs(cmdargs, "--threads", strconv.FormatInt(int64(dmgAttrs.nThreads), 10))
	}
	cmdargs = job.AddArgs(cmdargs, "--pixels", dmgAttrs.sourceImgPixels)
	cmdargs = job.AddArgs(cmdargs, "--labels", dmgAttrs.sourceImgLabels)
	cmdargs = job.AddArgs(cmdargs, "--out", dmgAttrs.destImg)
	cmdargs = job.AddArgs(cmdargs, "--temp", dmgAttrs.scratchDir)
	// !!!!! TODO
	return cmdargs, nil
}

func processJob(j job.Job) (job.Info, error) {
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
