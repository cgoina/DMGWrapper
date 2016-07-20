package job

import (
	"flag"
	"fmt"
	"strconv"
	"strings"

	"config"
)

// ValueList - type for a list of values
type ValueList []string

// String returns the stringified form of a valuelist
func (vl *ValueList) String() string {
	return strings.Join(*vl, ",")
}

// Set the value list from a string
func (vl *ValueList) Set(value string) error {
	values := strings.Split(value, ",")
	for _, v := range values {
		*vl = append(*vl, strings.Trim(v, " "))
	}
	return nil
}

// Get Value Getter method
func (vl *ValueList) Get() interface{} {
	return []string(*vl)
}

// FlagsCtor command flags constructor
type FlagsCtor interface {
	Name() string
	DefineArgs(fs *flag.FlagSet)
	IsHelpFlagSet() bool
}

// Args - command line arguments
type Args struct {
	Flags       *flag.FlagSet
	config      config.Config
	changedArgs flag.FlagSet
}

// NewArgs creates a argument set
func NewArgs(flagsCtor FlagsCtor) *Args {
	args := &Args{}
	args.Flags = flag.NewFlagSet(flagsCtor.Name(), flag.ExitOnError)
	flagsCtor.DefineArgs(args.Flags)
	return args
}

// GetArgValue return the value of the argument with the specified name
func (a Args) GetArgValue(name string) (v interface{}, err error) {
	f := a.changedArgs.Lookup(name)
	if f == nil {
		f = a.Flags.Lookup(name)
	}
	if f == nil {
		return 0, fmt.Errorf("No flag found for %s", name)
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("GetArgValue error: %v", r)
		}
	}()
	return f.Value.(flag.Getter).Get(), nil
}

// GetBoolArgValue retrieve argument's value as a bool
func (a Args) GetBoolArgValue(name string) (bool, error) {
	v, err := a.GetArgValue(name)
	if err != nil {
		return false, err
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("GetBoolArgValue error: %v", r)
		}
	}()
	return v.(bool), nil
}

// GetFloat64ArgValue retrieve argument's value as an float64
func (a Args) GetFloat64ArgValue(name string) (float64, error) {
	v, err := a.GetArgValue(name)
	if err != nil {
		return 0, err
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("GetIntArgValue error: %v", r)
		}
	}()
	return v.(float64), nil
}

// GetIntArgValue retrieve argument's value as an int
func (a Args) GetIntArgValue(name string) (int, error) {
	v, err := a.GetArgValue(name)
	if err != nil {
		return 0, err
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("GetIntArgValue error: %v", r)
		}
	}()
	return v.(int), nil
}

// GetStringArgValue retrieve argument's value as a string
func (a Args) GetStringArgValue(name string) (string, error) {
	v, err := a.GetArgValue(name)
	if err != nil {
		return "", err
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("GetStringArgValue error: %v", r)
		}
	}()
	return v.(string), nil
}

// UpdateIntArg
func (a *Args) UpdateIntArg(name string, value int) {
	f := a.changedArgs.Lookup(name)
	if f != nil {
		f.Value.Set(strconv.FormatInt(int64(value), 10))
	} else {
		valRef := a.changedArgs.Int(name, value, "")
		*valRef = value
	}
}

// AddArgs append the list of arguments to the current arglist
func AddArgs(arglist []string, args ...string) []string {
	return append(arglist, args...)
}

// AddArg a single name, value argument separated by 'separator' to the arglist
func AddArg(arglist []string, name, value, separator string) []string {
	if value != "" {
		newarglist := append(arglist, argFrom(name, value, separator))
		return newarglist
	}
	return arglist
}

// AddBoolArg add a boolean argument to the arglist
func AddBoolArg(arglist []string, name string, value bool, separator string) []string {
	if value {
		newarglist := append(arglist, argFrom(name, "true", separator))
		return newarglist
	}
	return arglist
}

// AddIntArg add an int argument to the arglist
func AddIntArg(arglist []string, name string, value int64, separator string) []string {
	if value >= 0 {
		newarglist := append(arglist, argFrom(name, strconv.FormatInt(value, 10), separator))
		return newarglist
	}
	return arglist
}

// AddFloatArg add a float argument to the arglist
func AddFloatArg(arglist []string, name string, value float64, prec, bitSize int, separator string) []string {
	if value > 0 {
		newarglist := append(arglist, argFrom(name, strconv.FormatFloat(value, 'f', prec, bitSize), separator))
		return newarglist
	}
	return arglist
}

func argFrom(name, value, separator string) string {
	return name + separator + value
}

// CmdlineArgBuilder creates command line arguments
type CmdlineArgBuilder interface {
	GetCmdlineArgs(a Args) ([]string, error)
}
