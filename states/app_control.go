package states

import (
	"context"
	"fmt"
	"sort"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/spf13/cobra"

	"github.com/milvus-io/birdwatcher/framework"
)

// getExitCmd returns exit command for input state.
func getExitCmd(state framework.State) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "exit",
		Short:   "Closes the cli",
		Aliases: []string{"quit"},
		RunE: func(*cobra.Command, []string) error {
			state.SetNext("", &exitState{})
			// cannot return ExitErr here to avoid print help message
			return nil
		},
	}
	return cmd
}

// exitState simple exit state.
type exitState struct {
	framework.CmdState
}

// SetupCommands setups the command.
// also called after each command run to reset flag values.
func (s *exitState) SetupCommands() {}

// IsEnding returns true for exit State
func (s *exitState) IsEnding() bool { return true }

func (app *ApplicationState) listStates() []string {
	statesNames := lo.Keys(app.states)
	sort.Strings(statesNames)
	return statesNames
}

type ListStatesParam struct {
	framework.ParamBase `use:"list states" desc:"list current connected states"`
}

func (app *ApplicationState) ListStatesCommand(ctx context.Context, p *ListStatesParam) error {
	statesNames := app.listStates()
	for _, stateName := range statesNames {
		fmt.Printf("%s\t%s\n", stateName, app.states[stateName].Label())
	}
	return nil
}

type DisconnectParam struct {
	framework.ParamBase `use:"disconnect" desc:"disconnect online states"`
	components          []string
}

func (p *DisconnectParam) ParseArgs(args []string) error {
	if len(args) == 0 {
		return errors.New("disconnect component not provided")
	}
	p.components = args
	return nil
}

// DisconnectCommand implements disconnect sub state logic.
func (app *ApplicationState) DisconnectCommand(ctx context.Context, p *DisconnectParam) error {
	for _, comp := range p.components {
		state, ok := app.states[comp]
		if !ok {
			fmt.Printf("State %s not connected.\n", comp)
			continue
		}
		state.Close()
		delete(app.states, comp)
		fmt.Printf("%s State disconnected.\n", comp)
	}
	return nil
}
