package bapps

import (
	"errors"

	"github.com/manifoldco/promptui"
	"github.com/milvus-io/birdwatcher/common"
	"github.com/milvus-io/birdwatcher/framework"
)

// simpleApp wraps promptui as BApp.
type simpleApp struct {
	currentState framework.State
}

func NewSimpleApp() BApp {
	return &simpleApp{}
}

// Run starts BirdWatcher with promptui. (disable suggestion and history)
func (a *simpleApp) Run(start framework.State) {
	app := start
	for {
		p := promptui.Prompt{
			Label: app.Label(),
			Validate: func(input string) error {
				return nil
			},
		}

		line, err := p.Run()
		if err == nil {
			app, err = app.Process(line)
			if errors.Is(err, common.ExitErr) {
				break
			}
			if app.IsEnding() {
				return
			}
			app.SetupCommands()
		}
	}
}
