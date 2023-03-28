package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"sort"
	"strings"
	"time"

	"github.com/c-bata/go-prompt"
	"github.com/manifoldco/promptui"
	_ "github.com/milvus-io/birdwatcher/asap"
	"github.com/milvus-io/birdwatcher/common"
	"github.com/milvus-io/birdwatcher/configs"
	"github.com/milvus-io/birdwatcher/history"
	"github.com/milvus-io/birdwatcher/states"
	"github.com/samber/lo"
)

var (
	simple       = flag.Bool("simple", false, "use simple ui without suggestion and history")
	printVersion = flag.Bool("version", false, "print version")
	logger       *log.Logger
)

func main() {
	flag.Parse()
	if *printVersion {
		fmt.Println("Birdwatcher Version", common.Version)
		return
	}

	defer handleExit()
	config, err := configs.NewConfig(".bw_config")
	if err != nil {
		// run by default, just printing warning.
		fmt.Println("[WARN] load config file failed", err.Error())
	}

	app := states.Start(config)
	// open file and create if non-existent
	file, err := os.OpenFile("bw_debug.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	logger = log.New(file, "Custom Log", log.LstdFlags)

	runPrompt(app, config)
}

func handleExit() {
	rawModeOff := exec.Command("/bin/stty", "-raw", "echo")
	rawModeOff.Stdin = os.Stdin
	_ = rawModeOff.Run()
	rawModeOff.Wait()
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
	exited          bool
	currentState    states.State
	sugguestHistory bool
	historyHelper   *history.HistoryHelper
}

// promptExecute actual execution logic entry.
func (a *promptApp) promptExecute(in string) {
	in = strings.TrimSpace(in)

	// try to get $PAGER env
	pager := os.Getenv("PAGER")

	pagerSig := make(chan struct{})
	stdout := os.Stdout

	var writer *os.File
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
		writer = w
		os.Stdout = w
		cmd.Stdin = r
		cmd.Stdout = stdout
		cmd.Stderr = os.Stderr

		err = cmd.Start()
		if err != nil {
			fmt.Printf("[WARNING] Cannot use %%PAGER(%s), set output back to stdout\n", pager)
			close(pagerSig)
		} else {
			go func() {
				// wait here in case of pager exit early
				cmd.Wait()
				logger.Printf("[DEBUG] wait pager done, state: %#v", cmd.ProcessState)
				// set to /dev/null to discard not wanted output
				os.Stdout, _ = os.Open(os.DevNull)
				w.Close()
				close(pagerSig)
			}()
		}
	} else {
		close(pagerSig)
	}
	a.currentState, _ = a.currentState.Process(in)
	if writer != nil {
		writer.Close()
	}
	<-pagerSig
	// recovery normal output
	os.Stdout = stdout
	// back to normal mode
	a.historyHelper.AddLog(in)
	a.sugguestHistory = false

	if a.currentState.IsEnding() {
		fmt.Println("Bye!")
		a.exited = true
	}
}

// completeInput auto-complete logic entry.
func (a *promptApp) completeInput(d prompt.Document) []prompt.Suggest {

	input := d.CurrentLineBeforeCursor()
	if a.sugguestHistory {
		return a.historySuggestions(input)
	}
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
	sort.Slice(s, func(i, j int) bool {
		return s[i].Text < s[j].Text
	})
	return s
}

func (a *promptApp) historySuggestions(input string) []prompt.Suggest {
	items := a.historyHelper.List(input)
	sort.Slice(items, func(i, j int) bool {
		return items[i].Ts > items[j].Ts
	})
	return lo.Map(items, func(item history.HistoryItem, _ int) prompt.Suggest {
		t := time.Unix(item.Ts, 0)
		return prompt.Suggest{
			Text:        item.Cmd,
			Description: t.Format("2006-01-02 15:03:04"),
		}
	})
}

// livePrefix implements dynamic change prefix.
func (a *promptApp) livePrefix() (string, bool) {
	if a.exited {
		return "", false
	}
	return fmt.Sprintf("%s > ", a.currentState.Label()), true
}

// runPrompt start BirdWatcher with go-prompt.
func runPrompt(app states.State, config *configs.Config) {
	// use workspace path to open&store history log
	hh := history.NewHistoryHelper(config.WorkspacePath)
	pa := &promptApp{currentState: app, historyHelper: hh}
	historyItems := hh.List("")
	sort.Slice(historyItems, func(i, j int) bool {
		return historyItems[i].Ts < historyItems[j].Ts
	})

	p := prompt.New(pa.promptExecute, pa.completeInput,
		prompt.OptionTitle("BirdWatcher"),
		prompt.OptionHistory(lo.Map(historyItems, func(hi history.HistoryItem, _ int) string { return hi.Cmd })),
		prompt.OptionLivePrefix(pa.livePrefix),
		prompt.OptionPrefixTextColor(prompt.Yellow),
		prompt.OptionPreviewSuggestionTextColor(prompt.Blue),
		prompt.OptionSelectedSuggestionBGColor(prompt.LightGray),
		prompt.OptionSuggestionBGColor(prompt.DarkGray),
		prompt.OptionSetExitCheckerOnInput(func(in string, breakline bool) bool {
			// setup exit command
			if strings.ToLower(in) == "exit" && breakline {
				return true
			}
			return false
		}),
		prompt.OptionAddKeyBind(prompt.KeyBind{
			Key: prompt.ControlR,
			Fn: func(buffer *prompt.Buffer) {
				pa.sugguestHistory = !pa.sugguestHistory
			},
		}),
	)
	p.Run()
}
