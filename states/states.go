package states

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"reflect"
	"strconv"
	"strings"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/autocomplete"
)

// State is the interface for application state.
type State interface {
	Ctx() (context.Context, context.CancelFunc)
	Label() string
	Process(cmd string) (State, error)
	Close()
	SetNext(state State)
	Suggestions(input string) map[string]string
	SetupCommands()
	IsEnding() bool
}

// cmdState is the basic state to process input command.
type cmdState struct {
	label     string
	rootCmd   *cobra.Command
	nextState State
	signal    <-chan os.Signal

	setupFn func()
}

// Ctx returns context which bind to sigint handler.
func (s *cmdState) Ctx() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer cancel()
		select {
		case <-s.signal:
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}

// SetupCommands perform command setup & reset.
func (s *cmdState) SetupCommands() {
	if s.setupFn != nil {
		s.setupFn()
	}
}

// mergeFunctionCommands parses all member methods for provided state and add it into cmd.
func (s *cmdState) mergeFunctionCommands(cmd *cobra.Command, state State) {
	items := parseFunctionCommands(state)
	for _, item := range items {
		target := cmd
		for _, kw := range item.kws {
			node, _, err := cmd.Find([]string{kw})
			if err != nil {
				newNode := &cobra.Command{Use: kw}
				target.AddCommand(newNode)
				node = newNode
			}
			target = node
		}
		target.AddCommand(item.cmd)
	}
}

// Label returns the display label for current cli.
func (s *cmdState) Label() string {
	return s.label
}

func (s *cmdState) Suggestions(input string) map[string]string {
	return autocomplete.SuggestInputCommands(input, s.rootCmd.Commands())
}

// Process is the main entry for processing command.
func (s *cmdState) Process(cmd string) (State, error) {
	args := strings.Split(cmd, " ")

	target, _, err := s.rootCmd.Find(args)
	if err == nil && target != nil {
		defer target.SetArgs(nil)
	}

	signal.Reset(syscall.SIGINT)
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT)
	s.signal = c

	s.rootCmd.SetArgs(args)
	err = s.rootCmd.Execute()
	signal.Reset(syscall.SIGINT)
	if errors.Is(err, ExitErr) {
		return s.nextState, ExitErr
	}
	if err != nil {
		return s, err
	}
	if s.nextState != nil {
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

// Check state is ending state.
func (s *cmdState) IsEnding() bool { return false }

type exitParam struct {
	framework.ParamBase `use:"exit" desc:"Close this CLI tool"`
}

// ExitCommand returns exit command
func (s *cmdState) ExitCommand(ctx context.Context, _ *exitParam) {
	s.SetNext(&exitState{})
}

type commandItem struct {
	kws []string
	cmd *cobra.Command
}

func parseFunctionCommands(state State) []commandItem {
	v := reflect.ValueOf(state)
	tp := v.Type()

	var commands []commandItem
	for i := 0; i < v.NumMethod(); i++ {
		mt := tp.Method(i)

		// parse method like with pattern %Command
		if !strings.HasSuffix(mt.Name, "Command") {
			continue
		}

		cmd, uses, ok := parseMethod(state, mt)
		if !ok {
			continue
		}

		commands = append(commands, commandItem{
			kws: uses[:len(uses)-1],
			cmd: cmd,
		})
	}

	return commands
}

func parseMethod(state State, mt reflect.Method) (*cobra.Command, []string, bool) {
	v := reflect.ValueOf(state)
	t := mt.Type
	var use string
	var short string
	var paramType reflect.Type

	if t.NumIn() == 0 {
		// shall not be reached
		return nil, nil, false
	}
	if t.NumIn() > 1 {
		// should be context.Context
		in := t.In(1)
		if !in.Implements(reflect.TypeOf((*context.Context)(nil)).Elem()) {
			return nil, nil, false
		}
	}
	if t.NumIn() > 2 {
		// should be CmdParam
		in := t.In(2)
		if !in.Implements(reflect.TypeOf((*framework.CmdParam)(nil)).Elem()) {
			return nil, nil, false
		}
		cp, ok := reflect.New(in.Elem()).Interface().(framework.CmdParam)
		if !ok {
			fmt.Println("conversion failed", in.Name())
		} else {
			paramType = in
			use, short = cp.Desc()
		}
	}

	// fmt.Println(mt.Name)
	cp := reflect.New(paramType.Elem()).Interface().(framework.CmdParam)
	fUse, fDesc := GetCmdFromFlag(cp)
	if len(use) == 0 {
		use = fUse
	}
	if len(short) == 0 {
		short = fDesc
	}
	if len(use) == 0 {
		fnName := mt.Name
		use = strings.ToLower(fnName[:len(fnName)-8])
	}
	uses := ParseUseSegments(use)
	lastKw := uses[len(uses)-1]

	cmd := &cobra.Command{
		Use: lastKw,
	}
	setupFlags(cp, cmd.Flags())
	cmd.Short = short
	cmd.Run = func(cmd *cobra.Command, args []string) {
		cp := reflect.New(paramType.Elem()).Interface().(framework.CmdParam)

		cp.ParseArgs(args)
		if err := parseFlags(cp, cmd.Flags()); err != nil {
			fmt.Println(err.Error())
			return
		}
		ctx, cancel := state.Ctx()
		defer cancel()

		m := v.MethodByName(mt.Name)
		results := m.Call([]reflect.Value{
			reflect.ValueOf(ctx),
			reflect.ValueOf(cp),
		})
		// reverse order, check error first
		for i := 0; i < len(results); i++ {
			result := results[len(results)-i-1]
			switch {
			case result.Type().Implements(reflect.TypeOf((*error)(nil)).Elem()):
				// error nil, skip
				if result.IsNil() {
					continue
				}
				err := result.Interface().(error)
				fmt.Println(err.Error())
				return
			case result.Type().Implements(reflect.TypeOf((*framework.ResultSet)(nil)).Elem()):
				if result.IsNil() {
					continue
				}
				rs := result.Interface().(framework.ResultSet)
				if preset, ok := rs.(*framework.PresetResultSet); ok {
					fmt.Println(preset.String())
					return
				}
				fmt.Println(rs.PrintAs(framework.FormatDefault))
			}
		}
	}
	return cmd, uses, true
}

func GetCmdFromFlag(p framework.CmdParam) (string, string) {
	v := reflect.ValueOf(p)
	if v.Kind() != reflect.Pointer {
		fmt.Println("param is not pointer")
		return "", ""
	}

	for v.Kind() != reflect.Struct {
		v = v.Elem()
	}
	tp := v.Type()

	f, has := tp.FieldByName("ParamBase")
	if !has {
		return "", ""
	}

	if f.Type.Kind() != reflect.Struct {
		return "", ""
	}

	tag := f.Tag
	return tag.Get("use"), tag.Get("desc")
}

func ParseUseSegments(use string) []string {
	parts := strings.Split(use, " ")
	last := ""
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if strings.HasPrefix(part, "[") && strings.HasSuffix(part, "]") {
			last = fmt.Sprintf("%s %s", last, part)
			continue
		}
		if len(last) > 0 {
			result = append(result, last)
		}
		last = part
	}
	if len(last) > 0 {
		result = append(result, last)
	}
	return result
}

func setupFlags(p framework.CmdParam, flags *pflag.FlagSet) {
	v := reflect.ValueOf(p)
	if v.Kind() != reflect.Pointer {
		fmt.Println("param is not pointer")
		return
	}

	for v.Kind() != reflect.Struct {
		v = v.Elem()
	}
	tp := v.Type()

	for i := 0; i < v.NumField(); i++ {
		f := tp.Field(i)
		if !f.IsExported() {
			continue
		}
		name := f.Tag.Get("name")
		defaultStr := f.Tag.Get("default")
		desc := f.Tag.Get("desc")
		switch f.Type.Kind() {
		case reflect.Int64:
			var dv int64
			if v, err := strconv.ParseInt(defaultStr, 10, 64); err == nil {
				dv = v
			}
			flags.Int64(name, dv, desc)
		case reflect.String:
			flags.String(name, defaultStr, desc)
		case reflect.Bool:
			var dv bool
			if v, err := strconv.ParseBool(defaultStr); err == nil {
				dv = v
			}
			flags.Bool(name, dv, desc)
		case reflect.Struct:
			continue
		default:
			fmt.Printf("field %s with kind %s not supported yet\n", f.Name, f.Type.Kind())
		}
	}
}

func parseFlags(p framework.CmdParam, flags *pflag.FlagSet) error {
	v := reflect.ValueOf(p)
	if v.Kind() != reflect.Pointer {
		return errors.New("param is not pointer")
	}

	v = v.Elem()
	tp := v.Type()

	for i := 0; i < v.NumField(); i++ {
		f := tp.Field(i)
		if !f.IsExported() {
			continue
		}
		name := f.Tag.Get("name")
		switch f.Type.Kind() {
		case reflect.Int64:
			p, err := flags.GetInt64(name)
			if err != nil {
				return err
			}
			v.FieldByName(f.Name).SetInt(p)
		case reflect.String:
			p, err := flags.GetString(name)
			if err != nil {
				return err
			}
			v.FieldByName(f.Name).SetString(p)
		case reflect.Bool:
			p, err := flags.GetBool(name)
			if err != nil {
				return err
			}
			v.FieldByName(f.Name).SetBool(p)
		case reflect.Struct:
			continue
		default:
			fmt.Printf("field %s with kind %s not supported yet\n", f.Name, f.Type.Kind())
		}
	}

	return nil
}
