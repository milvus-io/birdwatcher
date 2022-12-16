package autocomplete

type cmdCompType int

const (
	cmdCompAll cmdCompType = iota
	cmdCompCommand
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
	// raw is the complete value before component parsing
	raw string
	// cTag is command name for cmd, or flag name for cmdFlag
	cTag string
	// cValue is the flag value if any
	cValue string
	// cType marks the comp is command or flag
	cType cmdCompType
}
