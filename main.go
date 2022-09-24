package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/c-bata/go-prompt"
	"github.com/manifoldco/promptui"
	"github.com/milvus-io/birdwatcher/states"
)

var (
	simple = flag.Bool("simple", false, "use simple ui without suggestion and history")
)

func main() {
	app := states.Start()
	runPrompt(app)
}

// run start BirdWatcher with promptui. (disable suggestion and history)
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

// promptApp model wraps states to provide function for go-prompt.
type promptApp struct {
	currentState states.State
}

// promptExecute actual execution logic entry.
func (a *promptApp) promptExecute(in string) {
	in = strings.TrimSpace(in)
	var err error

	a.currentState, err = a.currentState.Process(in)
	if errors.Is(err, states.ExitErr) {
		os.Exit(0)
	}
}

// completeInput auto-complete logic entry.
func (a *promptApp) completeInput(d prompt.Document) []prompt.Suggest {
	input := d.CurrentLineBeforeCursor()
	if input == "" {
		return nil
	}
	r := a.currentState.Suggestions(input)
	s := make([]prompt.Suggest, 0, len(r))
	for usage, short := range r {
		s = append(s, prompt.Suggest{
			Text:        usage,
			Description: short,
		})
	}
	parts := strings.Split(input, " ")
	lastPart := parts[len(parts)-1]
	return prompt.FilterContains(s, lastPart, true)
}

// livePrefix implements dynamic change prefix.
func (a *promptApp) livePrefix() (string, bool) {
	return fmt.Sprintf("%s > ", a.currentState.Label()), true
}

// runPrompt start BirdWatcher with go-prompt.
func runPrompt(app states.State) {
	pa := &promptApp{currentState: app}
	p := prompt.New(pa.promptExecute, pa.completeInput,
		prompt.OptionTitle("BirdWatcher"),
		prompt.OptionHistory([]string{""}),
		prompt.OptionLivePrefix(pa.livePrefix),
		prompt.OptionPrefixTextColor(prompt.Yellow),
		prompt.OptionPreviewSuggestionTextColor(prompt.Blue),
		prompt.OptionSelectedSuggestionBGColor(prompt.LightGray),
		prompt.OptionSuggestionBGColor(prompt.DarkGray))
	p.Run()
}
