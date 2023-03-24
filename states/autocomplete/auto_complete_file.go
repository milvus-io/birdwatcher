package autocomplete

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"

	"github.com/mitchellh/go-homedir"
)

type fileCandidate struct {
	previousCandidates []acCandidate
	validator          func(file os.FileInfo) bool
}

func (c *fileCandidate) Match(input cComp) bool {
	return input.cType == cmdCompCommand
}

func (c *fileCandidate) NextCandidates(_ []acCandidate) []acCandidate {
	return c.previousCandidates
}

func (c *fileCandidate) Suggest(target cComp) map[string]string {

	ctag := target.cTag
	var err error
	if strings.HasPrefix(ctag, "~") {
		ctag, err = homedir.Expand(ctag)
		if err != nil {
			return map[string]string{}
		}
	}
	var d, part string
	if !strings.HasSuffix(ctag, "/") {
		d = path.Dir(ctag)
		part = path.Base(ctag)
	} else {
		d = ctag
		part = ""
	}
	if target.cType == cmdCompAll {
		return map[string]string{"file": fmt.Sprintf("folder/part: %s/%s", d, part)}
	}

	if debugSuggestion {
		fmt.Println(d, part)
	}
	parent, err := os.Stat(d)
	if err != nil {
		return map[string]string{}
	}
	if !parent.IsDir() {
		return map[string]string{}
	}

	fs, err := ioutil.ReadDir(d)
	if err != nil {
		return map[string]string{}
	}

	result := make(map[string]string)
	for _, f := range fs {
		if strings.HasPrefix(f.Name(), part) {
			if c.validator != nil && !c.validator(f) {
				continue
			}
			result[path.Join(d, f.Name())] = ""
		}
	}
	return result
}
