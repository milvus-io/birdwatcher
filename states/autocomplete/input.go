package autocomplete

import (
	"strings"

	"github.com/samber/lo"
)

func parseInput(input string) inputResult {
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
