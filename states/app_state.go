package states

import (
	"context"
	"fmt"
	"strings"

	"github.com/milvus-io/birdwatcher/configs"
	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/storage"
	"github.com/samber/lo"
	"github.com/spf13/cobra"
)

// ApplicationState application background state.
// used for state switch/merging.
type ApplicationState struct {
	// current state
	states map[string]framework.State

	root *cobra.Command
	core *framework.CmdState

	// config stores configuration items
	config *configs.Config
}

func (app *ApplicationState) Ctx() (context.Context, context.CancelFunc) {
	return app.core.Ctx()
}

func (app *ApplicationState) Label() string {
	if len(app.states) == 0 {
		return "Offline"
	}
	builder := &strings.Builder{}

	for _, name := range app.listStates() {
		builder.WriteString(app.states[name].Label())
	}
	return builder.String()
}

func (app *ApplicationState) Process(cmd string) (framework.State, error) {
	app.config.Logger.Println("[INFO] begin to process command", cmd)
	app.core.Process(cmd)
	// perform sub state transfer
	for key, state := range app.states {
		next := state.NextState()
		if next != nil {
			state.SetNext(nil)
			app.states[key] = next
		}
	}

	return app, nil
}

func (app *ApplicationState) Close() {
	for _, state := range app.states {
		state.Close()
	}
}

func (app *ApplicationState) SetNext(state framework.State) {
	app.config.Logger.Println("[WARNING] SetNext called for ApplicationState, which is not expected.")
}

func (app *ApplicationState) NextState() framework.State {
	return app
}

func (app *ApplicationState) SetTagNext(tag string, state framework.State) {
	app.states[tag] = state
}

func (app *ApplicationState) Suggestions(input string) map[string]string {
	result := make(map[string]string)
	states := append(lo.MapToSlice(app.states, func(_ string, state framework.State) framework.State {
		return state
	}), app.core)
	for _, state := range states {
		for k, v := range state.Suggestions(input) {
			result[k] = v
		}
	}
	return result
}

// SetupCommands implments framework.State.
// initialize or reset command after execution.
func (app *ApplicationState) SetupCommands() {
	cmd := app.core.GetCmd()

	app.core.UpdateState(cmd, app, app.SetupCommands)
	for _, state := range app.states {
		state.SetupCommands()
	}
}

func (app *ApplicationState) IsEnding() bool {
	for _, state := range app.states {
		if state.IsEnding() {
			return true
		}
	}
	return false
}

func (app *ApplicationState) ConnectMinioCommand(ctx context.Context, p *storage.ConnectMinioParam) error {
	state, err := storage.ConnectMinio(ctx, p)
	if err != nil {
		return err
	}

	app.SetTagNext(minioTag, state)
	return nil
}

type exitParam struct {
	framework.ParamBase `use:"exit" desc:"Close this CLI tool"`
}

func (app *ApplicationState) ExitCommand(ctx context.Context, _ *exitParam) {
	app.SetTagNext("exit", &exitState{})
}

type debugParam struct {
	framework.ParamBase `use:"debug commands" desc:"debug current command tree"`
}

func (app *ApplicationState) DebugCommand(ctx context.Context, p *debugParam) {
	for _, cmd := range app.core.RootCmd.Commands() {
		app.printCommands(cmd, 0)
	}
}

func (app *ApplicationState) printCommands(cmd *cobra.Command, level int) {
	for i := 0; i < level; i++ {
		fmt.Print("\t")
	}
	fmt.Println(cmd.Use)
	for _, subCmd := range cmd.Commands() {
		app.printCommands(subCmd, level+1)
	}
}
