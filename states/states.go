package states

import (
	"errors"
	"fmt"
	"strings"

	"github.com/milvus-io/birdwatcher/states/autocomplete"
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
	IsEnding() bool
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
	return autocomplete.SuggestInputCommands(input, s.rootCmd.Commands())
}

func printCommands(cmds []*cobra.Command) {
	for _, cmd := range cmds {
		fmt.Printf("\"%s\"", cmd.Use)
	}
	fmt.Println()
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
	if errors.Is(err, ExitErr) {
		return s.nextState, ExitErr
	}
	if err != nil {
		return s, err
	}
	if s.nextState != nil {
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

// Check state is ending state.
func (s *cmdState) IsEnding() bool { return false }
