package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"

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

func main() {
	var (
		err error
	)

	dmgAttrs := &dmg.Attrs{}
	cmdFlags := registerArgs()
	cmd := job.NewCmd(dmgAttrs)

	parseArgs(cmdFlags)
	if helpFlag {
		printDefaults(cmdFlags, cmd.CliArgs.Flags)
		os.Exit(0)
	}

	leftArgs := cmdFlags.NArg()
	if leftArgs < 1 {
		log.Println("Action is required")
		printDefaults(cmdFlags, cmd.CliArgs.Flags)
		os.Exit(1)
	}

	var jobAction job.ActionType
	jobAction = job.ActionType(os.Args[len(os.Args)-leftArgs])

	// parse the rest of the command line arguments
	cmd.CliArgs.Flags.Parse(os.Args[len(os.Args)-leftArgs+1:])

	if dmgAttrs.IsHelpFlagSet() {
		printDefaults(cmdFlags, cmd.CliArgs.Flags)
		os.Exit(0)
	}
	// read the configuration(s)
	resources, err := config.GetConfig(dmgAttrs.Configs...)
	if err != nil {
		log.Fatalf("Error reading the config file(s) %v: %v", dmgAttrs.Configs, err)
	}

	job := job.Job{
		Executable: resources.GetStringProperty("dmgServer"),
		Action:     jobAction,
		JArgs:      cmd.CliArgs,
	}
	dmgs := createDMGServer(*resources)
	jobInfo, err := dmgs.Process(job)
	if err != nil {
		log.Fatalf("Error invoking the DMG Server")
	}
	jobOutput, err := jobInfo.JobStdout()
	if err != nil {
		log.Printf("Error getting job's output")
	}
	r := bufio.NewReader(jobOutput)
	fmt.Printf("!!!! READ\n")
	line, n, err := r.ReadLine()
	fmt.Printf("!!!!! n=%d err=%v, BUF %s !!!!!\n", n, err, line)

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
