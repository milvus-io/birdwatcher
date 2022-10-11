package states

import (
	"fmt"
	"strings"

	"github.com/samber/lo"
	"github.com/spf13/cobra"
)

// State is the interface for application state.
type State interface {
	Label() string
	Process(cmd string) (State, error)
	Close()
	SetNext(state State)
	Suggestions(input string) map[string]string
	SetupCommands()
}

// cmdState is the basic state to process input command.
type cmdState struct {
	label     string
	rootCmd   *cobra.Command
	nextState State

	setupFn func()
}

func (s *cmdState) SetupCommands() {
	if s.setupFn != nil {
		s.setupFn()
	}
}

// Label returns the display label for current cli.
func (s *cmdState) Label() string {
	return s.label
}

func (s *cmdState) Suggestions(input string) map[string]string {
	iResult := s.parseInput(input)

	return findCmdSuggestions(iResult.parts, s.rootCmd.Commands())
}

func printCommands(cmds []*cobra.Command) {
	for _, cmd := range cmds {
		fmt.Printf("\"%s\"", cmd.Use)
	}
	fmt.Println()
}

func (s *cmdState) parseInput(input string) inputResult {
	// check is end with space
	isEndBlank := strings.HasSuffix(input, " ")

	parts := strings.Split(input, " ")
	parts = lo.Filter(parts, func(part string, idx int) bool {
		return part != ""
	})

	comps := make([]cComp, 0, len(parts))
	currentFlagValue := false

	for _, part := range parts {
		// next part is flag value
		// just set last comp cValue
		if currentFlagValue {
			comps[len(comps)-1].cValue = part
			currentFlagValue = false
			continue
		}
		// is flag
		if strings.HasPrefix(part, "-") {
			raw := strings.TrimLeft(part, "-")
			if strings.Contains(raw, "=") {
				parts := strings.Split(raw, "=")
				// a=b
				if len(parts) == 2 {
					comps = append(comps, cComp{
						raw:    part,
						cTag:   parts[0],
						cValue: parts[1],
						cType:  cmdCompFlag,
					})
				}
				// TODO handle part len != 2
			} else {
				currentFlagValue = true
				comps = append(comps, cComp{
					raw:   part,
					cTag:  raw,
					cType: cmdCompFlag,
				})
			}
		} else {
			comps = append(comps, cComp{
				raw:   part,
				cTag:  part,
				cType: cmdCompCommand,
			})
		}
	}

	is := inputStateCmd
	if currentFlagValue {
		if isEndBlank {
			is = inputStateFlagValue
		} else {
			is = inputStateFlagTag
		}
	}

	// add empty comp if end with space
	if isEndBlank {
		comps = append(comps, cComp{cType: cmdCompCommand})
	}

	return inputResult{
		parts: comps,
		state: is,
	}
}

// Process is the main entry for processing command.
func (s *cmdState) Process(cmd string) (State, error) {
	args := strings.Split(cmd, " ")

	target, _, err := s.rootCmd.Find(args)
	if err == nil && target != nil {
		defer target.SetArgs(nil)
	}
	s.rootCmd.SetArgs(args)
	err = s.rootCmd.Execute()
	if err != nil {
		return s, err
	}
	if s.nextState != nil {
		//defer s.Close()
		// TODO fix ugly type cast
		if _, ok := s.nextState.(*exitState); ok {
			return s.nextState, ExitErr
		}
		nextState := s.nextState
		s.nextState = nil
		return nextState, nil
	}

	// reset command states
	s.SetupCommands()
	return s, nil
}

// SetNext simple method to set next state.
func (s *cmdState) SetNext(state State) {
	s.nextState = state
}

// Close empty method to implement State.
func (s *cmdState) Close() {}

// Start returns the first state - offline.
func Start() State {
	root := &cobra.Command{
		Use:   "",
		Short: "",
	}

	state := &cmdState{
		label:   "Offline",
		rootCmd: root,
	}

	root.AddCommand(
		// connect
		getConnectCommand(state),
		// load-backup
		getLoadBackupCmd(state),
		// exit
		getExitCmd(state))

	root.AddCommand(getGlobalUtilCommands()...)

	return state
}
