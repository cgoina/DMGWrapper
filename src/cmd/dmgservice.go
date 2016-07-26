package main

import (
	"flag"
	"fmt"
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
	dmgProcessorType string
	helpFlag         bool
)

type serviceFunc func() error

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
	if leftArgs < 1 {
		log.Println("Missing operation")
		printDefaults(cmdFlags, cmdArgs.Flags)
		os.Exit(2)
	}
	operation := os.Args[len(os.Args)-leftArgs]

	firstJobArgIndex := len(os.Args) - leftArgs + 1
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

	service, err := createDMGService(operation, dmgProcessorType, cmdArgs, *resources)
	if err != nil {
		log.Fatalf("Error creating the DMG service: %v", err)
	}
	if err = service(); err != nil {
		log.Fatalf("Error running the DMG service: %v", err)
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

	fs.StringVar(&sessionName, "sessionName", "mipmaps", "Grid job session name")
	fs.StringVar(&jobName, "jobName", "mipmaps", "Job name")
	fs.StringVar(&accountingID, "A", "", "Grid account id")
	fs.BoolVar(&destroySession, "destroySession", false, "If true it destroyes the session when it's done if no errors have been encountered")
	fs.StringVar(&dmgProcessorType, "dmgProcessor", "drmaa1", "Job processor type {local, drmaa1, drmaa2}")
	fs.BoolVar(&helpFlag, "h", false, "Display command line usage flags")
	return fs
}

func printDefaults(fs ...*flag.FlagSet) {
	for _, f := range fs {
		f.SetOutput(nil)
		f.PrintDefaults()
	}
}

func createDMGProcessor(resources config.Config) (process.Processor, error) {
	var dmgProcessor process.Processor
	var err error
	switch dmgProcessorType {
	case "echo":
		dmgProcessor = process.NewEchoProcessor()
	case "local":
		dmgProcessor = &dmg.LocalDmgProcessor{Resources: resources}
	case "drmaa1":
		dmgProcessor, err = drmaautils.NewGridProcessor(sessionName,
			accountingID,
			drmaautils.NewDRMAAV1Proxy(),
			resources)
	case "drmaa2":
		dmgProcessor, err = drmaautils.NewGridProcessor(sessionName,
			accountingID,
			drmaautils.NewDRMAAV2Proxy(),
			resources)
	default:
		err = fmt.Errorf("Invalid DMG processor type: %s", dmgProcessorType)
	}
	return dmgProcessor, err
}

func createDMGService(operation string,
	dmgProcessorType string,
	args *arg.Args,
	resources config.Config) (serviceFunc, error) {
	var err error
	dmgProcessor, err := createDMGProcessor(resources)
	if err != nil {
		return nil, err
	}

	bandsProcessor := dmg.ImageBandsProcessor{
		ImageProcessor: dmgProcessor,
		Resources:      resources,
	}

	switch operation {
	case "dmgImage":
		return serviceFunc(func() error {
			j := process.Job{
				JArgs: args.Clone(),
			}
			return bandsProcessor.Run(j)
		}), nil

	case "dmgSection":
		return serviceFunc(func() error {
			var sectionPreparer dmg.SectionPreparer
			sectionArgs, err := sectionPreparer.CreateSectionJobArgs(args, resources)
			if err != nil {
				return err
			}

			j := process.Job{
				JArgs: *sectionArgs,
			}
			if err := bandsProcessor.Run(j); err != nil {
				return err
			}
			return nil
		}), nil
	default:
		return nil, fmt.Errorf("Invalid DMG operation: %s. Supported values are:{dmgType, dmgSection}",
			dmgProcessorType)
	}
}
