package states

import "github.com/spf13/cobra"

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
			return nil
		},
	}
	return cmd
}

// exitState simple exit state.
type exitState struct {
	cmdState
}
