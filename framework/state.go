package framework

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/milvus-io/birdwatcher/common"
	"github.com/milvus-io/birdwatcher/states/autocomplete"
)

// State is the interface for application state.
type State interface {
	Ctx() (context.Context, context.CancelFunc)
	Label() string
	Process(cmd string) (State, error)
	Close()
	SetNext(state State)
	NextState() State
	Suggestions(input string) map[string]string
	SetupCommands()
	IsEnding() bool
}

// SetupFunc function type for setup commands.
type SetupFunc func()

// CmdState wraps cobra command as State interface.
type CmdState struct {
	parent    *CmdState
	label     string
	RootCmd   *cobra.Command
	nextState State
	signal    <-chan os.Signal

	SetupFn func()
}

// NewCmdState returns a CmdState with provided label.
func NewCmdState(label string) *CmdState {
	return &CmdState{
		label: label,
	}
}

// SetLabel updates label value.
func (s *CmdState) SetLabel(label string) {
	s.label = label
}

// Spawn returns a child command connected to current state as parent.
func (s *CmdState) Spawn(label string) *CmdState {
	p := s
	for p.parent != nil {
		p = p.parent
	}
	return &CmdState{
		parent: p,
		label:  label,
	}
}

// GetCmd returns the command instance for SetupCommands().
// if parent presents and root cmd not nil, use parent command.
// otherwise, create a new cobra.Command.
func (s *CmdState) GetCmd() *cobra.Command {
	if s.parent != nil && s.parent.RootCmd != nil {
		return s.parent.RootCmd
	}

	return &cobra.Command{}
}

func (s *CmdState) UpdateState(cmd *cobra.Command, state State, fn SetupFunc) {
	s.MergeFunctionCommands(cmd, state)
	s.RootCmd = cmd
	s.SetupFn = fn
}

// Ctx returns context which bind to sigint handler.
func (s *CmdState) Ctx() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer cancel()
		select {
		case <-s.signal:
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}

// SetupCommands perform command setup & reset.
func (s *CmdState) SetupCommands() {
	if s.SetupFn != nil {
		s.SetupFn()
	}
}

// MergeFunctionCommands parses all member methods for provided state and add it into cmd.
func (s *CmdState) MergeFunctionCommands(cmd *cobra.Command, state State) {
	items := parseFunctionCommands(state)
	for _, item := range items {
		target := cmd
		for _, kw := range item.kws {
			node, _, err := target.Find([]string{kw})
			if err != nil || (node != nil && node.Use == "") {
				newNode := &cobra.Command{Use: kw}
				target.AddCommand(newNode)
				node = newNode
			}
			target = node
		}
		target.AddCommand(item.cmd)
	}
}

func (s *CmdState) MergeCobraCommands(base *cobra.Command, cmds ...*cobra.Command) {
	for _, cmd := range cmds {
		target, _, err := base.Find([]string{cmd.Use})
		if err != nil || (target != nil && target.Use == base.Use) {
			base.AddCommand(cmd)
			continue
		}
		s.MergeCobraCommands(target, cmd.Commands()...)
	}
}

// Label returns the display label for current cli.
func (s *CmdState) Label() string {
	return s.label
}

func (s *CmdState) Suggestions(input string) map[string]string {
	return autocomplete.SuggestInputCommands(input, s.RootCmd.Commands())
}

// Process is the main entry for processing command.
func (s *CmdState) Process(cmd string) (State, error) {
	args := strings.Split(cmd, " ")

	target, _, err := s.RootCmd.Find(args)
	if err == nil && target != nil {
		defer target.SetArgs(nil)
	}

	signal.Reset(syscall.SIGINT)
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT)
	s.signal = c

	s.RootCmd.SetArgs(args)
	err = s.RootCmd.Execute()
	signal.Reset(syscall.SIGINT)

	if errors.Is(err, common.ExitErr) {
		return s.nextState, common.ExitErr
	}
	if err != nil {
		return s, err
	}
	if s.nextState != nil {
		nextState := s.nextState
		s.nextState = nil
		return nextState, nil
	}

	return s, nil
}

// SetNext simple method to set next state.
func (s *CmdState) SetNext(state State) {
	s.nextState = state
}

func (s *CmdState) NextState() State {
	return s.nextState
}

// Close empty method to implement State.
func (s *CmdState) Close() {}

// Check state is ending state.
func (s *CmdState) IsEnding() bool { return false }
