package main

import (
	"flag"
	"io/ioutil"
	"log"
	"os"

	"arg"
	"config"
	"dmg"
	"drmaautils"
	"process"
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
	cmdArgs := arg.NewArgs(dmgAttrs)

	if len(os.Args) == 1 {
		printDefaults(cmdFlags, cmdArgs.Flags)
		os.Exit(2)
	}

	parseArgs(cmdFlags)
	if helpFlag {
		printDefaults(cmdFlags, cmdArgs.Flags)
		os.Exit(0)
	}

	leftArgs := cmdFlags.NArg()
	firstJobArgIndex := len(os.Args) - leftArgs - 1
	if firstJobArgIndex < 1 {
		firstJobArgIndex = 1
	}
	// parse the rest of the command line arguments
	cmdArgs.Flags.Parse(os.Args[firstJobArgIndex:])

	if dmgAttrs.IsHelpFlagSet() {
		printDefaults(cmdFlags, cmdArgs.Flags)
		os.Exit(0)
	}
	// read the configuration(s)
	resources, err := config.GetConfig(dmgAttrs.Configs...)
	if err != nil {
		log.Fatalf("Error reading the config file(s) %v: %v", dmgAttrs.Configs, err)
	}

	var dmgProcessor process.Processor
	switch jobProcessorType {
	case "local":
		dmgProcessor = &dmg.LocalDmgProcessor{*resources}
	case "drmaa1":
		dmgProcessor, err = drmaautils.NewGridProcessor(sessionName,
			accountingID,
			drmaautils.NewDRMAAV1Proxy(),
			*resources)
		if err != nil {
			log.Fatalf("Error instantiating the DMG Server")
		}
	case "drmaa2":
		dmgProcessor, err = drmaautils.NewGridProcessor(sessionName,
			accountingID,
			drmaautils.NewDRMAAV2Proxy(),
			*resources)
		if err != nil {
			log.Fatalf("Error instantiating the DMG Server")
		}
	}
	dmgService := dmg.Service{
		DMGProcessor: dmgProcessor,
		Resources:    *resources,
	}
	if err = dmgService.ProcessDMG(cmdArgs); err != nil {
		log.Fatalf("Error during DMG processing: %v", err)
	}
}

// parseArgs parses the command line arguments up to the first unknown one.
// The method recovers from the panic and allow the other command to continue parsing the rest of
// the arguments from where it left off.
func parseArgs(fs *flag.FlagSet) error {
	defer func() {
		recover()
	}()

	return fs.Parse(os.Args[1:])
}

// registerArgs registers command specific arguments.
func registerArgs() (fs *flag.FlagSet) {
	fs = flag.NewFlagSet("submitJobs", flag.ContinueOnError)
	fs.SetOutput(ioutil.Discard)

	fs.StringVar(&sessionName, "session_name", "mipmaps", "Grid job session name")
	fs.StringVar(&jobName, "job_name", "mipmaps", "Job name")
	fs.StringVar(&accountingID, "A", "", "Grid account id")
	fs.BoolVar(&destroySession, "destroy_session", false, "If true it destroyes the session when it's done if no errors have been encountered")
	fs.StringVar(&jobProcessorType, "job_processor", "drmaa1", "Job processor type {local, drmaa1, drmaa2}")
	fs.BoolVar(&helpFlag, "h", false, "Display command line usage flags")
	return fs
}

func printDefaults(fs ...*flag.FlagSet) {
	for _, f := range fs {
		f.SetOutput(nil)
		f.PrintDefaults()
	}
}
