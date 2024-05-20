package bapps

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"sort"
	"strings"
	"time"

	"github.com/c-bata/go-prompt"
	"github.com/milvus-io/birdwatcher/configs"
	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/history"
	"github.com/samber/lo"
)

// PromptApp wraps go-prompt as application.
type PromptApp struct {
	exited          bool
	currentState    framework.State
	sugguestHistory bool
	historyHelper   *history.Helper
	logger          *log.Logger
	prompt          *prompt.Prompt
	config          *configs.Config
}

func NewPromptApp(config *configs.Config, opts ...AppOption) BApp {
	opt := &appOption{}
	for _, o := range opts {
		o(opt)
	}

	config.SetLogger(opt.logger)

	// use workspace path to open&store history log
	hh := history.NewHistoryHelper(config.WorkspacePath)
	pa := &PromptApp{
		historyHelper: hh,
		config:        config,
	}
	pa.logger = opt.logger

	historyItems := hh.List("")
	sort.Slice(historyItems, func(i, j int) bool {
		return historyItems[i].Ts < historyItems[j].Ts
	})

	p := prompt.New(pa.promptExecute, pa.completeInput,
		prompt.OptionTitle("BirdWatcher"),
		prompt.OptionHistory(lo.Map(historyItems, func(hi history.Item, _ int) string { return hi.Cmd })),
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
		// setup InputParser with `TearDown` overrided
		prompt.OptionParser(NewBInputParser()),
	)
	pa.prompt = p
	return pa
}

func (a *PromptApp) Run(start framework.State) {
	a.currentState = start
	a.prompt.Run()
}

// promptExecute actual execution logic entry.
func (a *PromptApp) promptExecute(in string) {
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
				if a.logger != nil {
					a.logger.Printf("[DEBUG] wait pager done, state: %#v", cmd.ProcessState)
				}
				// set to /dev/null to discard not wanted output
				os.Stdout, _ = os.Open(os.DevNull)
				w.Close()
				close(pagerSig)
			}()
		}
	} else {
		os.Stdout = stdout
		close(pagerSig)
	}
	nextState, err := a.currentState.Process(in)
	if writer != nil {
		writer.Close()
	}
	<-pagerSig
	// recovery normal output
	os.Stdout = stdout
	// back to normal mode
	a.historyHelper.AddLog(in)
	a.sugguestHistory = false

	if err != nil {
		fmt.Println(err.Error())
		return
	}

	nextState.SetupCommands()
	a.currentState = nextState

	if a.currentState.IsEnding() {
		fmt.Println("Bye!")
		a.exited = true
	}
}

// completeInput auto-complete logic entry.
func (a *PromptApp) completeInput(d prompt.Document) []prompt.Suggest {

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

// historySuggestions returns suggestion from command history.
func (a *PromptApp) historySuggestions(input string) []prompt.Suggest {
	items := a.historyHelper.List(input)
	sort.Slice(items, func(i, j int) bool {
		return items[i].Ts > items[j].Ts
	})

	lastIdx := strings.LastIndex(input, " ") + 1
	return lo.Map(items, func(item history.Item, _ int) prompt.Suggest {
		t := time.Unix(item.Ts, 0)
		return prompt.Suggest{
			Text:        item.Cmd[lastIdx:],
			Description: t.Format("2006-01-02 15:03:04"),
		}
	})
}

// livePrefix implements dynamic change prefix.
func (a *PromptApp) livePrefix() (string, bool) {
	if a.exited {
		return "", false
	}
	return fmt.Sprintf("%s > ", a.currentState.Label()), true
}
