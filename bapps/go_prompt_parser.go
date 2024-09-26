package bapps

import (
	"os"
	"os/exec"

	"github.com/c-bata/go-prompt"
)

// bInputParser wraps prompt.PosixParser to change TearDown behavior.
type bInputParser struct {
	*prompt.PosixParser
}

// TearDown should be called after stopping input
func (t *bInputParser) TearDown() error {
	t.PosixParser.TearDown()
	rawModeOff := exec.Command("/bin/stty", "-raw", "echo")
	rawModeOff.Stdin = os.Stdin
	_ = rawModeOff.Run()
	rawModeOff.Wait()
	return nil
}

func NewBInputParser() *bInputParser {
	p := &bInputParser{
		PosixParser: prompt.NewStandardInputParser(),
	}
	return p
}
