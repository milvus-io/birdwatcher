package states

import (
	"strings"

	"github.com/spf13/cobra"
)

// State is the interface for application state.
type State interface {
	Label() string
	Process(cmd string) (State, error)
	Close()
	SetNext(state State)
	Suggestions(input string) map[string]string
	SetupCommands()
}

// cmdState is the basic state to process input command.
type cmdState struct {
	label     string
	rootCmd   *cobra.Command
	nextState State

	setupFn func()
}

func (s *cmdState) SetupCommands() {
	if s.setupFn != nil {
		s.setupFn()
	}
}

// Label returns the display label for current cli.
func (s *cmdState) Label() string {
	return s.label
}

func (s *cmdState) Suggestions(input string) map[string]string {
	result := make(map[string]string)
	subCmds := s.rootCmd.Commands()
	for _, subCmd := range subCmds {
		result[subCmd.Use] = subCmd.Short
	}
	return result
}

// Process is the main entry for processing command.
func (s *cmdState) Process(cmd string) (State, error) {
	args := strings.Split(cmd, " ")

	target, _, err := s.rootCmd.Find(args)
	if err == nil && target != nil {
		defer target.SetArgs(nil)
	}
	s.rootCmd.SetArgs(args)
	err = s.rootCmd.Execute()
	if err != nil {
		return s, err
	}
	if s.nextState != nil {
		//defer s.Close()
		// TODO fix ugly type cast
		if _, ok := s.nextState.(*exitState); ok {
			return s.nextState, ExitErr
		}
		nextState := s.nextState
		s.nextState = nil
		return nextState, nil
	}

	// reset command states
	s.SetupCommands()
	return s, nil
}

// SetNext simple method to set next state.
func (s *cmdState) SetNext(state State) {
	s.nextState = state
}

// Close empty method to implement State.
func (s *cmdState) Close() {}

// Start returns the first state - offline.
func Start() State {
	root := &cobra.Command{
		Use:   "",
		Short: "",
	}

	state := &cmdState{
		label:   "Offline",
		rootCmd: root,
	}

	root.AddCommand(
		// connect
		getConnectCommand(state),
		// load-backup
		getLoadBackupCmd(state),
		// exit
		getExitCmd(state))

	root.AddCommand(getGlobalUtilCommands()...)

	return state
}
