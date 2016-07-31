package cmdutils

import (
	"flag"
	"fmt"
	"os"

	"config"
	"drmaautils"
	"process"
)

// ParseArgs parses the command line arguments up to the first unknown one.
// The method recovers from the panic and allow the other command to continue parsing the rest of
// the arguments from where it left off.
func ParseArgs(fs *flag.FlagSet) error {
	defer func() {
		recover()
	}()

	return fs.Parse(os.Args[1:])
}

// CreateProcessor create the job processor
func CreateProcessor(processorType, accountID, sessionName string,
	localProcessorCtor func() (process.Processor, error),
	resources config.Config) (process.Processor, error) {
	var p process.Processor
	var err error
	switch processorType {
	case "echo":
		p = process.NewEchoProcessor()
	case "local":
		p, err = localProcessorCtor()
	case "drmaa1":
		p, err = drmaautils.NewGridProcessor(sessionName,
			accountID,
			drmaautils.NewDRMAAV1Proxy(),
			resources)
	case "drmaa2":
		p, err = drmaautils.NewGridProcessor(sessionName,
			accountID,
			drmaautils.NewDRMAAV2Proxy(),
			resources)
	default:
		err = fmt.Errorf("Invalid processor type: '%s'. Supported types are: {echo, local,drmaa1, drmaa2}", processorType)
	}
	return p, err
}
