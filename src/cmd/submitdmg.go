package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"config"
	"dmg"
	"drmaautils"
	"job"
)

var (
	sessionName      string
	jobName          string
	accountingID     string
	destroySession   bool
	jobProcessorType string
	helpFlag         bool
)

const (
	pauseBetweenChecksInSec = 10
	maxChecks               = 100
	serverAddressPrefix     = "Server Address: "
)

func main() {
	var (
		err error
	)

	dmgAttrs := &dmg.Attrs{}
	cmdFlags := registerArgs()
	cmdArgs := job.NewArgs(dmgAttrs)

	parseArgs(cmdFlags)
	if helpFlag {
		printDefaults(cmdFlags, cmdArgs.Flags)
		os.Exit(0)
	}

	leftArgs := cmdFlags.NArg()
	if leftArgs < 1 {
		log.Println("Action is required")
		printDefaults(cmdFlags, cmdArgs.Flags)
		os.Exit(1)
	}

	var jobAction job.ActionType
	jobAction = job.ActionType(os.Args[len(os.Args)-leftArgs])

	// parse the rest of the command line arguments
	cmdArgs.Flags.Parse(os.Args[len(os.Args)-leftArgs+1:])

	if dmgAttrs.IsHelpFlagSet() {
		printDefaults(cmdFlags, cmdArgs.Flags)
		os.Exit(0)
	}
	// read the configuration(s)
	resources, err := config.GetConfig(dmgAttrs.Configs...)
	if err != nil {
		log.Fatalf("Error reading the config file(s) %v: %v", dmgAttrs.Configs, err)
	}

	job := job.Job{
		Executable:     resources.GetStringProperty("dmgServer"),
		Action:         jobAction,
		JArgs:          *cmdArgs,
		CmdlineBuilder: dmg.ServerCmdlineBuilder{},
	}
	dmgs := createDMGServer(*resources)
	jobInfo, serverAddress, err := startDMGServer(dmgs, job)
	if err != nil {
		log.Fatalf("Error waiting for the DMG Server to start: %v", err)
	}
	log.Printf("Found Server Address: %s", serverAddress)
	if err = jobInfo.WaitForTermination(); err != nil {
		log.Fatalf("Error waiting for the DMG Server to terminate")
	}
}

// parseArgs parses the command line arguments up to the first unknown one.
// The method recovers from the panic and allow the other command to continue parsing the rest of
// the arguments from where it left off.
func parseArgs(fs *flag.FlagSet) {
	defer func() {
		recover()
	}()

	fs.Parse(os.Args[1:])
}

// registerArgs registers command specific arguments.
func registerArgs() (fs *flag.FlagSet) {
	fs = flag.NewFlagSet("submitJobs", flag.PanicOnError)

	fs.StringVar(&sessionName, "session_name", "mipmaps", "Grid job session name")
	fs.StringVar(&jobName, "job_name", "mipmaps", "Job name")
	fs.StringVar(&accountingID, "A", "", "Grid account id")
	fs.BoolVar(&destroySession, "destroy_session", false, "If true it destroyes the session when it's done if no errors have been encountered")
	fs.StringVar(&jobProcessorType, "job_processor", "drmaa1", "Job processor type {local, drmaa1, drmaa2, print}")
	fs.BoolVar(&helpFlag, "h", false, "Display command line usage flags")
	return fs
}

func printDefaults(fs ...*flag.FlagSet) {
	for _, f := range fs {
		f.PrintDefaults()
	}
}

func createDMGServer(resources config.Config) job.Processor {
	dmgs, err := drmaautils.NewGridProcessor(sessionName, accountingID, drmaautils.NewDRMAAV1Proxy(), resources)
	if err != nil {
		log.Fatalf("Error instantiating the DMG Server")
	}
	return dmgs
}

func startDMGServer(sp job.Processor, j job.Job) (job.Info, string, error) {
	jobInfo, err := sp.Process(j)
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
			fmt.Printf("!!!!! SERVER ADDRESS IS:%s!!!!\n", serverAddress)
			return jobInfo, serverAddress, nil
		}
		time.Sleep(pauseBetweenChecksInSec * time.Second)
	}
	return jobInfo, "", fmt.Errorf("Timeout - could not read server's address")
}
