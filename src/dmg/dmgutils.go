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
	Configs               job.ValueList
	helpFlag              bool
	serverHost            string
	serverPort            int
	nSections             int
	iterations, vCycles   int
	verbose, gray, deramp bool
	tileExt               string
	sourceImg, destImg    string
}

// Name method
func (a *Attrs) Name() string {
	return "dmg"
}

const (
	defaultPort = 11000
)

// DefineArgs method
func (a *Attrs) DefineArgs(fs *flag.FlagSet) {
	fs.Var(&a.Configs, "config", "list of configuration files which applied in the order they are specified")
	fs.IntVar(&a.nSections, "sections", 1, "Number of sections processed in parallel")
	fs.IntVar(&a.iterations, "iters", 5, "Number of Gauss-Siebel iterations")
	fs.IntVar(&a.vCycles, "vCycles", 1, "Number of V-cycles")
	fs.StringVar(&a.serverHost, "serverHost", "localhost", "DMG server host")
	fs.IntVar(&a.serverPort, "serverPort", defaultPort, "DMG server port")
	fs.BoolVar(&a.verbose, "verbose", false, "verbosity flag")
	fs.BoolVar(&a.gray, "gray", true, "gray image flag")
	fs.BoolVar(&a.deramp, "deramp", true, "deramp flag")
	fs.StringVar(&a.tileExt, "tileExt", "png", "Destination image extension")
	fs.BoolVar(&a.helpFlag, "h", false, "gray image flag")
}

// IsHelpFlagSet method
func (a *Attrs) IsHelpFlagSet() bool {
	return a.helpFlag
}

func (a *Attrs) extractDmgAttrs(ja job.Args) (err error) {
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
	if a.verbose, err = ja.GetBoolArgValue("verbose"); err != nil {
		return err
	}
	if a.gray, err = ja.GetBoolArgValue("gray"); err != nil {
		return err
	}
	if a.deramp, err = ja.GetBoolArgValue("deramp"); err != nil {
		return err
	}
	if a.tileExt, err = ja.GetStringArgValue("tileExt"); err != nil {
		return err
	}
	return nil
}

// localCmdInfo local process info
type localCmdInfo struct {
	cmd        *exec.Cmd
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
func (ls LocalDmgServer) Process(j job.Job) (job.JobInfo, error) {
	cmdargs, err := prepareServerArgs(j.JArgs)
	if err != nil {
		return nil, fmt.Errorf("Error preparing the command line arguments: %v", err)
	}
	cmd := exec.Command(ls.Resources.GetStringProperty("dmgServer"), cmdargs...)
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
		cmd:        cmd,
		jobStdout: stdout,
		jobStderr: stderr,
	}
	err = cmd.Start()
	return lci, err
}

func prepareServerArgs(a job.Args) ([]string, error) {
	var cmdargs []string
	var err error
	var dmgAttrs Attrs

	if err = dmgAttrs.extractDmgAttrs(a); err != nil {
		return cmdargs, err
	}
	cmdargs = job.AddArgs(cmdargs, "--port", strconv.FormatInt(int64(dmgAttrs.serverPort), 10))
	cmdargs = job.AddArgs(cmdargs, "--count", strconv.FormatInt(int64(dmgAttrs.nSections), 10))
	cmdargs = job.AddArgs(cmdargs, "--iters", strconv.FormatInt(int64(dmgAttrs.iterations), 10))
	cmdargs = job.AddArgs(cmdargs, "--vCycles", strconv.FormatInt(int64(dmgAttrs.vCycles), 10))
	cmdargs = job.AddArgs(cmdargs, "--tileExt", dmgAttrs.tileExt)
	if dmgAttrs.verbose {
		cmdargs = job.AddArgs(cmdargs, "--verbose")
	}
	if dmgAttrs.gray {
		cmdargs = job.AddArgs(cmdargs, "--gray")
	}
	if dmgAttrs.deramp {
		cmdargs = job.AddArgs(cmdargs, "--deramp")
	}
	// !!!!! TODO
	return cmdargs, nil
}

// LocalDmgClient - in charge with starting a DMG Client process
type LocalDmgClient struct {
	Resources config.Config
}

// Process launches the client
func (ls LocalDmgClient) Process(j job.Job) error {
	// !!!! TODO
	return nil
}
