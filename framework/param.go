package framework

// CmdParam is the interface definition for command parameter.
type CmdParam interface {
	ParseArgs(args []string) error
	Desc() (string, string)
}

// ParamBase implmenet CmdParam when empty args parser.
type ParamBase struct{}

func (pb ParamBase) ParseArgs(args []string) error {
	return nil
}

func (pb ParamBase) Desc() (string, string) {
	return "", ""
}
