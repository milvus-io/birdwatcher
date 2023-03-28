package bapps

import (
	"errors"

	"github.com/manifoldco/promptui"
	"github.com/milvus-io/birdwatcher/states"
)

// simpleApp wraps promptui as BApp.
type simpleApp struct {
	currentState states.State
}

func NewSimpleApp() BApp {
	return &simpleApp{}
}

// Run starts BirdWatcher with promptui. (disable suggestion and history)
func (a *simpleApp) Run(start states.State) {
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
			if errors.Is(err, states.ExitErr) {
				break
			}
			if app.IsEnding() {
				return
			}
		}
	}
}
