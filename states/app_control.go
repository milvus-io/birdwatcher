package states

import (
	"context"
	"fmt"
	"sort"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"

	"github.com/milvus-io/birdwatcher/framework"
)

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
	if len(p.components) == 0 {
		fmt.Println("component not provided, disconnect shall provided state tag after v1.1.0")
		fmt.Println("state type(s) connected now: ", app.listStates())
		return nil
	}
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
