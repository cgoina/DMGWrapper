package arg

import (
	"flag"
	"fmt"
	"strconv"
	"strings"

	"config"
)

// StringList - type for a list of strings
type StringList []string

// String returns the stringified form of a valuelist
func (vl *StringList) String() string {
	return strings.Join(*vl, ",")
}

// Set the value list from a string
func (vl *StringList) Set(value string) error {
	values := strings.Split(value, ",")
	for _, v := range values {
		*vl = append(*vl, strings.Trim(v, " "))
	}
	return nil
}

// Get Value Getter method
func (vl *StringList) Get() interface{} {
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
	changedArgs map[string]interface{}
}

// NewArgs creates a argument set
func NewArgs(flagsCtor FlagsCtor) *Args {
	args := &Args{
		Flags:       flag.NewFlagSet(flagsCtor.Name(), flag.ExitOnError),
		changedArgs: make(map[string]interface{}),
	}
	flagsCtor.DefineArgs(args.Flags)
	return args
}

// Clone - clones the current arguments
func (a *Args) Clone() Args {
	cloneArgs := Args{
		Flags:       a.Flags,
		config:      a.config,
		changedArgs: make(map[string]interface{}),
	}
	for k, v := range a.changedArgs {
		cloneArgs.changedArgs[k] = v
	}
	return cloneArgs
}

// GetArgValue return the value of the argument with the specified name
func (a Args) GetArgValue(name string) (v interface{}, err error) {
	v = a.changedArgs[name]
	if v != nil {
		return v, nil
	}
	f := a.Flags.Lookup(name)
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
			err = fmt.Errorf("GetBoolArgValue %s error: %v", name, r)
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
			err = fmt.Errorf("GetIntArgValue %s error: %v", name, r)
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
			err = fmt.Errorf("GetIntArgValue %s error: %v", name, r)
		}
	}()
	return v.(int), nil
}

// GetInt64ArgValue retrieve argument's value as an int64
func (a Args) GetInt64ArgValue(name string) (int64, error) {
	v, err := a.GetArgValue(name)
	if err != nil {
		return 0, err
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("GetInt64ArgValue %s error: %v", name, r)
		}
	}()
	return v.(int64), nil
}

// GetUintArgValue retrieve argument's value as an uint
func (a Args) GetUintArgValue(name string) (uint, error) {
	v, err := a.GetArgValue(name)
	if err != nil {
		return 0, err
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("GetUintArgValue %s error: %v", name, r)
		}
	}()
	return v.(uint), nil
}

// GetStringArgValue retrieve argument's value as a string
func (a Args) GetStringArgValue(name string) (string, error) {
	v, err := a.GetArgValue(name)
	if err != nil {
		return "", err
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("GetStringArgValue %s error: %v", name, r)
		}
	}()
	return v.(string), nil
}

// GetStringListArgValue return argument's value as a list of strings
func (a Args) GetStringListArgValue(name string) ([]string, error) {
	v, err := a.GetArgValue(name)
	if err != nil {
		return make([]string, 0), err
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("GetStringListArgValue %s error: %v", name, r)
		}
	}()
	return v.([]string), nil
}

// UpdateIntArg set the int value for the named argument
func (a *Args) UpdateIntArg(name string, value int) {
	a.changedArgs[name] = value
}

// UpdateInt64Arg set the int64 value for the named argument
func (a *Args) UpdateInt64Arg(name string, value int64) {
	a.changedArgs[name] = value
}

// UpdateUintArg set the uint value for the named argument
func (a *Args) UpdateUintArg(name string, value uint) {
	a.changedArgs[name] = value
}

// UpdateStringArg set the string value for the named argument
func (a *Args) UpdateStringArg(name string, value string) {
	a.changedArgs[name] = value
}

// UpdateStringListArg set the string list value for the named argument
func (a *Args) UpdateStringListArg(name string, value []string) {
	a.changedArgs[name] = value
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

// AddUintArg add an uint argument to the arglist
func AddUintArg(arglist []string, name string, value uint64, separator string) []string {
	if value >= 0 {
		newarglist := append(arglist, argFrom(name, strconv.FormatUint(value, 10), separator))
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

// DefaultIfEmpty returns the val if non empty otherwise it returns the default value
func DefaultIfEmpty(val, defaultVal string) string {
	if val == "" {
		return defaultVal
	}
	return val
}

// CmdlineArgBuilder creates command line arguments
type CmdlineArgBuilder interface {
	GetCmdlineArgs(a Args) ([]string, error)
}
