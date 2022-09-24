package states

type cmdCompType int

const (
	cmdCompCommand cmdCompType = iota + 1
	cmdCompFlag
)

type inputState int

const (
	inputStateCmd inputState = iota + 1
	inputStateFlagTag
	inputStateFlagValue
)

type inputResult struct {
	parts []cComp
	state inputState
}

type cComp struct {
	// cTag is command name for cmd, or flag name for cmdFlag
	cTag string
	// cValue is the flag value if any
	cValue string
	// cType marks the comp is command or flag
	cType cmdCompType
}
