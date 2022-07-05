package main

import (
	"errors"

	"github.com/congqixia/birdwatcher/states"
	"github.com/manifoldco/promptui"
)

func main() {
	app := states.Start()
	run(app)
}

func run(app states.State) {
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
		}
	}
}
