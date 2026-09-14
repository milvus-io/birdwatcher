package framework

import (
	"github.com/spf13/pflag"
)

// CmdParam is the interface definition for command parameter.
type CmdParam interface {
	ParseArgs(args []string) error
	Desc() (string, string)
}

// FlagSetAware is an optional interface for CmdParam implementations that need
// access to the parsed pflag.FlagSet, e.g. to detect whether a flag was
// explicitly set by the user.
type FlagSetAware interface {
	SetFlagSet(fs *pflag.FlagSet)
}

// ParamBase implmenet CmdParam when empty args parser.
type ParamBase struct{}

func (pb ParamBase) ParseArgs(args []string) error {
	return nil
}

func (pb ParamBase) Desc() (string, string) {
	return "", ""
}

// DataSetParam is the base for commands that return a ResultSet.
// It provides a built-in Format flag for output formatting.
type DataSetParam struct {
	ParamBase
	Format string `name:"format" default:"" desc:"output format" values:"default,json,table,line,plain"`
}

// GetFormat returns the Format as a framework.Format enum value.
func (p *DataSetParam) GetFormat() Format {
	return NameFormat(p.Format)
}

// ExecutionParam is the base for commands that modify metadata.
// It provides a built-in Run flag for dry-run support.
type ExecutionParam struct {
	ParamBase
	Run bool `name:"run" default:"false" desc:"actually execute the operation, default is dry-run"`
}

// IsDryRun returns true if the command should run in dry-run mode.
func (p *ExecutionParam) IsDryRun() bool {
	return !p.Run
}
