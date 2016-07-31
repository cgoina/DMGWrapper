package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"arg"
	"cmdutils"
	"config"
	"dmg"
	"process"
)

var (
	sessionName          string
	jobName              string
	accountID            string
	destroySession       bool
	dmgProcessorType     string
	sectionProcessorType string
	helpFlag             bool
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
		arg.PrintDefaults(cmdFlags, cmdArgs.Flags)
		os.Exit(2)
	}

	cmdutils.ParseArgs(cmdFlags)
	if helpFlag {
		arg.PrintDefaults(cmdFlags, cmdArgs.Flags)
		os.Exit(0)
	}

	leftArgs := cmdFlags.NArg()
	if leftArgs < 1 {
		log.Println("Missing operation")
		arg.PrintDefaults(cmdFlags, cmdArgs.Flags)
		os.Exit(2)
	}
	operation := os.Args[len(os.Args)-leftArgs]

	firstJobArgIndex := len(os.Args) - leftArgs + 1
	// parse the rest of the command line arguments
	cmdArgs.Flags.Parse(os.Args[firstJobArgIndex:])

	if dmgAttrs.IsHelpFlagSet() {
		arg.PrintDefaults(cmdFlags, cmdArgs.Flags)
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

// registerArgs registers command specific arguments.
func registerArgs() (fs *flag.FlagSet) {
	fs = flag.NewFlagSet("submitJobs", flag.ContinueOnError)
	fs.SetOutput(ioutil.Discard)

	fs.StringVar(&sessionName, "sessionName", "dmg", "Grid job session name")
	fs.StringVar(&jobName, "jobName", "dmg", "Job name")
	fs.StringVar(&accountID, "A", "", "Grid account id")
	fs.BoolVar(&destroySession, "destroySession", false, "If true it destroyes the session when it's done if no errors have been encountered")
	fs.StringVar(&dmgProcessorType, "dmgProcessor", "drmaa1", "Job processor type {echo, local, drmaa1, drmaa2}")
	fs.StringVar(&sectionProcessorType, "sectionProcessor", "local", "Job processor type {echo, local, drmaa1, drmaa2}")
	fs.BoolVar(&helpFlag, "h", false, "Display command line usage flags")
	return fs
}

func createDMGService(operation string,
	dmgProcessorType string,
	args *arg.Args,
	resources config.Config) (serviceFunc, error) {
	var err error
	dmgProcessor, err := cmdutils.CreateProcessor(dmgProcessorType,
		accountID,
		sessionName,
		func() (process.Processor, error) {
			return process.NewLocalCmdProcessor(resources), nil
		},
		resources)
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
				Name:  jobName,
				JArgs: args.Clone(),
			}
			return bandsProcessor.Run(j)
		}), nil
	case "dmgSection":
		return serviceFunc(func() error {
			sectionProcessor, err := cmdutils.CreateProcessor(sectionProcessorType,
				accountID,
				sessionName,
				func() (process.Processor, error) {
					return &dmg.SectionProcessor{
						ImageProcessor:   bandsProcessor,
						Resources:        resources,
						DMGProcessorType: dmgProcessorType,
					}, nil
				},
				resources)
			if err != nil {
				return err
			}
			j := process.Job{
				Executable: resources.GetStringProperty("dmgexec"),
				Name:       jobName,
				JArgs:      *args,
				CmdlineBuilder: dmg.SectionJobCmdlineBuilder{
					Operation:            "dmgSection",
					DMGProcessorType:     dmgProcessorType,
					SectionProcessorType: "local",
					ClusterAccountID:     accountID,
					SessionName:          sessionName,
					JobName:              fmt.Sprintf("%s-section", jobName),
				},
			}
			return sectionProcessor.Run(j)
		}), nil
	default:
		return nil, fmt.Errorf("Invalid DMG operation: %s. Supported values are:{dmgType, dmgSection}",
			dmgProcessorType)
	}
}
