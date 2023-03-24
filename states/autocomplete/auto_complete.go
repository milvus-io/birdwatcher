package autocomplete

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

var (
	// TODO add flags
	debugSuggestion = false
	aliasSuggestion = false
)

// acCandidate is the interface for auto-complete candidates.
type acCandidate interface {
	Match(cComp) bool
	NextCandidates([]acCandidate) []acCandidate
	Suggest(cComp) map[string]string
}

// cmdCandidate wraps cobra.Command as acCandidate.
type cmdCandidate struct {
	*cobra.Command
}

func (c *cmdCandidate) cmdName() string {
	return strings.Split(c.Use, " ")[0]
}

func (c *cmdCandidate) args() []acCandidate {
	parts := strings.Split(c.Use, " ")
	var result []acCandidate
	for _, part := range parts {
		if strings.HasPrefix(part, "[") && strings.HasSuffix(part, "]") {
			name := part[1 : len(part)-1]
			if name == "file" {
				result = append(result, &fileCandidate{previousCandidates: []acCandidate{}})
			}
			if name == "directory" {
				result = append(result, &fileCandidate{previousCandidates: []acCandidate{}, validator: func(info os.FileInfo) bool { return info.IsDir() }})
			}
		}
	}
	return result
}

// Match implements acCandidate, compares use string first part.
func (c *cmdCandidate) Match(input cComp) bool {
	if debugSuggestion {
		fmt.Printf("cmd: %s, cType: %d, cTag: %s\n", c.cmdName(), input.cType, input.cTag)
	}
	return input.cType == cmdCompCommand && c.cmdName() == input.cTag
}

// NextCandidates implements acCandidate, returns all subCommand and flags.
func (c *cmdCandidate) NextCandidates(_ []acCandidate) []acCandidate {
	var cmdFlags []*pflag.Flag
	c.Flags().VisitAll(func(flag *pflag.Flag) {
		cmdFlags = append(cmdFlags, flag)
	})
	subCommands := c.Commands()

	result := make([]acCandidate, 0, len(cmdFlags)+len(subCommands))
	for _, cmd := range subCommands {
		result = append(result, &cmdCandidate{Command: cmd})
	}
	for _, flag := range cmdFlags {
		result = append(result, &flagCandidate{Flag: flag})
	}
	result = append(result, c.args()...)

	return result
}

func (c *cmdCandidate) Suggest(target cComp) map[string]string {
	k := c.cmdName()
	if strings.HasPrefix(k, target.cTag) || target.cType == cmdCompAll {
		return map[string]string{c.cmdName(): c.Short}
	}
	return map[string]string{}
}

// flagCandidate wraps pflag.Flag as acCandidate.
type flagCandidate struct {
	*pflag.Flag
}

// Match implements acCandidate.
func (c *flagCandidate) Match(input cComp) bool {
	switch {
	// --flag
	case input.cType == cmdCompFlag:
		return input.cTag == c.Name
	// -s (short-hand)
	// TODO
	default:
		return false
	}
}

func (c *flagCandidate) Suggest(target cComp) map[string]string {
	k := fmt.Sprintf("--%s", c.Name)
	if (strings.HasPrefix(k, target.raw) && strings.HasPrefix(target.raw, "--")) || target.cType == cmdCompAll {
		return map[string]string{k: c.Usage}
	}
	return map[string]string{}
}

// NextCandidates iomplement acCandidate.
func (c *flagCandidate) NextCandidates(current []acCandidate) []acCandidate {
	// TODO add next value match all candidate
	return current
}

// SuggestInputCommands returns suggestions based on command setup.
func SuggestInputCommands(input string, commands []*cobra.Command) map[string]string {
	iResult := parseInput(input)

	return findCmdSuggestions(iResult.parts, commands)
}

func findCmdSuggestions(comps []cComp, commands []*cobra.Command) map[string]string {
	// no suggestion if input is empty
	if len(comps) == 0 {
		return map[string]string{}
	}

	// wraps commands into cmdCandidates
	var candidates []acCandidate
	for _, cmd := range commands {
		candidates = append(candidates, &cmdCandidate{Command: cmd})
	}

	if debugSuggestion {
		fmt.Println()
	}

	// reduce leading components
	// for example
	// "show segment --collection 123", ac target shall be "123"
	// "show segment", ac target shall be "segment"
loop:
	for i := 0; i < len(comps)-1; i++ {
		if debugSuggestion {
			fmt.Printf("reducing part %d:", i)
			printCandidates(candidates)
			fmt.Println(comps)
		}

		for _, candidate := range candidates {
			if candidate.Match(comps[i]) {
				candidates = candidate.NextCandidates(candidates)
				continue loop
			}
		}
		if debugSuggestion {
			fmt.Println("no suggestion matched, return")
		}
		return map[string]string{}
	}

	target := comps[len(comps)-1]
	if debugSuggestion {
		fmt.Println("target candidates")
		printCandidates(candidates)
		fmt.Println("target:", target)
	}
	// check candidates has target prefix
	result := make(map[string]string)
	for _, candidate := range candidates {
		suggests := candidate.Suggest(target)
		for k, v := range suggests {
			result[k] = v
		}
	}

	return result
}

func printCandidates(candidates []acCandidate) {
	for _, cmd := range candidates {
		suggests := cmd.Suggest(cComp{})
		for k := range suggests {
			fmt.Printf("\"%s\" ", k)
		}
	}
	fmt.Println()
}
