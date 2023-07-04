package states

import (
	"context"

	"github.com/spf13/cobra"
)

// ExitErr is the error indicates user needs to exit application.
var ExitErr = exitErr{}

// exitErr internal err type for comparing.
type exitErr struct{}

// Error implements error.
func (e exitErr) Error() string {
	return "exited"
}

// getExitCmd returns exit command for input state.
func getExitCmd(state State) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "exit",
		Short:   "Closes the cli",
		Aliases: []string{"quit"},
		RunE: func(*cobra.Command, []string) error {
			state.SetNext(&exitState{})
			// cannot return ExitErr here to avoid print help message
			return nil
		},
	}
	return cmd
}

// exitState simple exit state.
type exitState struct {
	cmdState
}

// SetupCommands setups the command.
// also called after each command run to reset flag values.
func (s *exitState) SetupCommands() {}

// IsEnding returns true for exit State
func (s *exitState) IsEnding() bool { return true }

type disconnectParam struct {
	ParamBase `use:"disconnect" desc:"disconnect from current etcd instance"`
}

func (s *instanceState) DisconnectCommand(ctx context.Context, _ *disconnectParam) {
	s.SetNext(Start(s.config))
	s.Close()
}
