package common

// ExitState simple exit state.
type ExitState struct {
	CmdState
}

// SetupCommands setups the command.
// also called after each command run to reset flag values.
func (s *ExitState) SetupCommands() {}

// IsEnding returns true for exit State
func (s *ExitState) IsEnding() bool { return true }
