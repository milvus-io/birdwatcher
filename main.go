package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/c-bata/go-prompt"
	"github.com/manifoldco/promptui"
	_ "github.com/milvus-io/birdwatcher/asap"
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

	// try to get $PAGER env
	pager := os.Getenv("PAGER")
	if pager != "" {
		var args []string
		// refine less behavior
		if pager == "less" {
			args = append(args,
				"-F",        // don't page if content can fix in one screen
				"--no-init", // don't clean screen when start paging
			)
		}
		// #nosec args audit for less
		cmd := exec.Command(pager, args...)

		r, w, err := os.Pipe()
		if err != nil {
			fmt.Println("failed to create os pipeline", err.Error())
			return
		}

		// Capture STDOUT for the Pager. Keep the old
		// value so we can restore it later.
		stdout := os.Stdout
		os.Stdout = w
		cmd.Stdin = r
		cmd.Stdout = stdout
		cmd.Stderr = os.Stderr

		err = cmd.Start()
		if err != nil {
			fmt.Printf("[WARNING] Cannot use %%PAGER(%s), set output back to stdout\n", pager)
			os.Stdout = stdout
		} else {
			defer func() {
				w.Close()
				os.Stdout = stdout
				cmd.Wait()
			}()
		}
	}

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
	return s
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
