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
	"mipmaps"
	"process"
)

var (
	sessionName          string
	jobName              string
	accountID            string
	destroySession       bool
	mipmapsProcessorType string
	helpFlag             bool
)

type serviceFunc func() error

func main() {
	var (
		err error
	)

	mipmapsAttrs := &mipmaps.Attrs{}

	cmdFlags := registerArgs()
	cmdArgs := arg.NewArgs(mipmapsAttrs)

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

	if mipmapsAttrs.IsHelpFlagSet() {
		arg.PrintDefaults(cmdFlags, cmdArgs.Flags)
		os.Exit(0)
	}
	// read the configuration(s)
	resources, err := config.GetConfig(mipmapsAttrs.Configs...)
	if err != nil {
		log.Fatalf("Error reading the config file(s) %v: %v", mipmapsAttrs.Configs, err)
	}

	service, err := createMipmapsService(operation, mipmapsProcessorType, cmdArgs, *resources)
	if err != nil {
		log.Fatalf("Error creating the DMG service: %v", err)
	}

	if err = service(); err != nil {
		log.Fatalf("Error running the Mipmaps service: %v", err)
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
	fs.StringVar(&mipmapsProcessorType, "mipmapsProcessor", "drmaa1", "Job processor type {echo, local, drmaa1, drmaa2}")
	fs.BoolVar(&helpFlag, "h", false, "Display command line usage flags")
	return fs
}

func createMipmapsService(operation string,
	mipmapsProcessorType string,
	args *arg.Args,
	resources config.Config) (serviceFunc, error) {
	var err error
	var dvidProxies mipmaps.DVIDProxyURLMapping

	mipmapsProcessor, err := cmdutils.CreateProcessor(mipmapsProcessorType,
		accountID,
		sessionName,
		func() (process.Processor, error) {
			// start DVID proxies
			if dvidProxies, err = mipmaps.StartDVIDProxies(resources); err != nil {
				return nil, err
			}
			return process.NewLocalCmdProcessor(resources), nil
		},
		resources)
	if err != nil {
		return nil, err
	}

	switch operation {
	case "retile":
		return serviceFunc(func() error {
			var retileProcessor process.Processor
			j := process.Job{
				Name:  jobName,
				JArgs: args.Clone(),
			}
			if mipmapsProcessorType == "local" {
				j.Executable = resources.GetStringProperty("jvm")
				j.CmdlineBuilder = mipmaps.NewLocalRetileCmdlineBuilder(resources, dvidProxies)
				retileProcessor = mipmapsProcessor
			} else {
				j.Executable = resources.GetStringProperty("mipmapsExec")
				j.Name = jobName + "_" + operation
				retileProcessor = process.NewParallelProcessor(mipmapsProcessor, mipmaps.NewRetileJobSplitter(resources), resources)
			}
			return retileProcessor.Run(j)
		}), nil
	case "scale":
		return serviceFunc(func() error {
			var scaleProcessor process.Processor
			j := process.Job{
				Name:  jobName,
				JArgs: args.Clone(),
			}
			if mipmapsProcessorType == "local" {
				j.Executable = resources.GetStringProperty("jvm")
				j.CmdlineBuilder = mipmaps.NewLocalScaleCmdlineBuilder(resources, dvidProxies)
				scaleProcessor = mipmapsProcessor
			} else {
				j.Executable = resources.GetStringProperty("mipmapsExec")
				j.Name = jobName + "_" + operation
				scaleProcessor = process.NewParallelProcessor(mipmapsProcessor, mipmaps.NewRetileJobSplitter(resources), resources)
			}
			return scaleProcessor.Run(j)
		}), nil
	case "fullPyramid":
		return serviceFunc(func() error {
			var jobs []process.Job
			retileCmdlineBuilder, err := mipmaps.NewServiceCmdlineBuilder(mipmapsProcessorType, "retile", resources)
			if err != nil {
				return err
			}
			jobs = append(jobs, process.Job{
				Executable:     resources.GetStringProperty("mipmapsExec"),
				Name:           jobName,
				JArgs:          args.Clone(),
				CmdlineBuilder: retileCmdlineBuilder,
			})
			scaleCmdlineBuilder, err := mipmaps.NewServiceCmdlineBuilder(mipmapsProcessorType, "scale", resources)
			if err != nil {
				return err
			}
			jobs = append(jobs, process.Job{
				Executable:     resources.GetStringProperty("mipmapsExec"),
				Name:           jobName,
				JArgs:          args.Clone(),
				CmdlineBuilder: scaleCmdlineBuilder,
			})
			return processPipelinedJobs(mipmapsProcessorType, resources, jobs)
		}), nil
	case "orthoviews":
	case "fullOrthoviews":
	default:
		return nil, fmt.Errorf("Unknown operation %s. Valid values are: retile | scale | fullPyramid | orthoviews | fullOrthoviews", operation)
	}
	return nil, err
}

func processPipelinedJobs(mipmapsProcessorType string, resources config.Config, jobs []process.Job) error {
	mipmapsProcessor, err := cmdutils.CreateProcessor(mipmapsProcessorType,
		accountID,
		sessionName,
		func() (process.Processor, error) {
			return process.NewLocalCmdProcessor(resources), nil
		},
		resources)
	if err != nil {
		return err
	}
	for _, job := range jobs {
		if err := mipmapsProcessor.Run(job); err != nil {
			return fmt.Errorf("Error encountered while processing job %s: %v", job.Name, err)
		}
	}
	return nil
}
