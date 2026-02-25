package autocomplete

import "strings"

// argSuggesterCandidate wraps a named ValueSuggester as an acCandidate for positional arguments.
type argSuggesterCandidate struct {
	name               string
	previousCandidates []acCandidate
}

func (c *argSuggesterCandidate) Match(input cComp) bool {
	return input.cType == cmdCompCommand
}

func (c *argSuggesterCandidate) NextCandidates(_ cComp, _ []acCandidate) []acCandidate {
	return c.previousCandidates
}

func (c *argSuggesterCandidate) Suggest(target cComp) map[string]string {
	s, ok := GetValueSuggester(c.name)
	if !ok {
		return map[string]string{}
	}
	result := make(map[string]string)
	for _, v := range s.Suggest(target.cTag) {
		if strings.HasPrefix(v, target.cTag) || target.cType == cmdCompAll {
			result[v] = ""
		}
	}
	return result
}
