package bapps

import (
	"os"
	"os/exec"
	"reflect"
	"unsafe"

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

func NewInputParser(parser *prompt.PosixParser) *bInputParser {
	p := &bInputParser{
		PosixParser: prompt.NewStandardInputParser(),
	}
	return p
}

func HandleFD(p *prompt.Prompt) error {
	in, ok := GetUnexportedField(reflect.ValueOf(p).Elem().FieldByName("in")).(*prompt.PosixParser)
	if !ok {
		// failed to reflect
		return nil
	}

	return prompt.OptionParser(NewInputParser(in))(p)
}

func GetUnexportedField(field reflect.Value) interface{} {
	return reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().Interface()
}
