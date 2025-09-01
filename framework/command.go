package framework

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

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
		if !in.Implements(reflect.TypeOf((*CmdParam)(nil)).Elem()) {
			return nil, nil, false
		}
		cp, ok := reflect.New(in.Elem()).Interface().(CmdParam)
		if !ok {
			fmt.Println("conversion failed", in.Name())
		} else {
			paramType = in
			use, short = cp.Desc()
		}
	}

	cp := reflect.New(paramType.Elem()).Interface().(CmdParam)
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
	cmd.RunE = func(cmd *cobra.Command, args []string) (err error) {
		cp := reflect.New(paramType.Elem()).Interface().(CmdParam)

		cp.ParseArgs(args)
		if err = parseFlags(cp, cmd.Flags()); err != nil {
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
				err = result.Interface().(error)
				fmt.Println(err.Error())
				return
			case result.Type().Implements(reflect.TypeOf((*ResultSet)(nil)).Elem()):
				if result.IsNil() {
					continue
				}
				rs := result.Interface().(ResultSet)
				if preset, ok := rs.(*PresetResultSet); ok {
					fmt.Println(preset.String())
					return
				}
				fmt.Println(rs.PrintAs(FormatDefault))
			}
		}
		return nil
	}
	return cmd, uses, true
}

func GetCmdFromFlag(p CmdParam) (string, string) {
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

// setupFlags performs command flag setup with CmdParam provided information.
func setupFlags(p CmdParam, flags *pflag.FlagSet) {
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
		case reflect.Slice:
			switch f.Type.Elem().Kind() {
			case reflect.Int64:
				flags.Int64Slice(name, []int64{}, desc)
			case reflect.String:
				flags.StringSlice(name, []string{}, desc)
			default:
				fmt.Printf("field %s with slice kind %s not supported yet\n", f.Name, f.Type.Elem().Kind())
			}
		default:
			fmt.Printf("field %s with kind %s not supported yet\n", f.Name, f.Type.Kind())
		}
	}
}

// parseFlags parse parameters from flagset and setup value via reflection.
func parseFlags(p CmdParam, flags *pflag.FlagSet) error {
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
		case reflect.Slice:
			// fmt.Println(f.Type.Elem())
			var p any
			var err error
			switch f.Type.Elem().Kind() {
			case reflect.Int64:
				p, err = flags.GetInt64Slice(name)
			case reflect.String:
				p, err = flags.GetStringSlice(name)
			default:
				fmt.Printf("field %s with slice kind %s not supported yet\n", f.Name, f.Type.Elem().Kind())
				continue
			}
			if err != nil {
				return err
			}
			v.FieldByName(f.Name).Set(reflect.ValueOf(p))
		default:
			fmt.Printf("field %s with kind %s not supported yet\n", f.Name, f.Type.Kind())
		}
	}

	return nil
}
