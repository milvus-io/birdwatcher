package bapps

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/milvus-io/birdwatcher/common"
	"github.com/milvus-io/birdwatcher/configs"
	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
)

type WebServerApp struct {
	port   int
	config *configs.Config
}

type InstanceInfo struct {
	EtcdAddr string `form:"etcd"`
	RootPath string `form:"rootPath"`
}

func (app *WebServerApp) Run(framework.State) {
	r := gin.Default()
	etcdversion.SetVersion(models.GTEVersion2_2)

	r.GET("/version", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"version": common.Version})
	})

	app.ParseRouter(r, &states.InstanceState{})

	r.Run(fmt.Sprintf(":%d", app.port))
}

func (app *WebServerApp) ParseRouter(r *gin.Engine, s framework.State) {
	v := reflect.ValueOf(s)
	tp := v.Type()

	for i := 0; i < v.NumMethod(); i++ {
		mt := tp.Method(i)

		// parse method like with pattern %Command
		if !strings.HasSuffix(mt.Name, "Command") {
			continue
		}

		// fmt.Println("parsing method", mt.Name)
		app.parseMethod(r, mt, mt.Name)
	}
}

func (app *WebServerApp) parseMethod(r *gin.Engine, mt reflect.Method, name string) {
	// v := reflect.ValueOf(s)
	t := mt.Type
	var use string
	var paramType reflect.Type

	if t.NumIn() == 0 {
		// shall not be reached
		return
	}
	if t.NumIn() > 1 {
		// should be context.Context
		in := t.In(1)
		if !in.Implements(reflect.TypeOf((*context.Context)(nil)).Elem()) {
			return
		}
	}
	if t.NumIn() > 2 {
		// should be CmdParam
		in := t.In(2)
		if !in.Implements(reflect.TypeOf((*framework.CmdParam)(nil)).Elem()) {
			return
		}
		cp, ok := reflect.New(in.Elem()).Interface().(framework.CmdParam)
		if !ok {
			fmt.Println("conversion failed", in.Name())
		} else {
			paramType = in
			use, _ = cp.Desc()
		}
	}
	if t.NumOut() == 0 {
		fmt.Printf("%s not output\n", name)
		return
	}

	if t.NumOut() > 0 {
		// should be ResultSet
		out := t.Out(0)
		if !out.Implements(reflect.TypeOf((*framework.ResultSet)(nil)).Elem()) {
			fmt.Printf("%s output not ResultSet\n", name)
			return
		}
	}

	//fmt.Println(mt.Name)
	cp := reflect.New(paramType.Elem()).Interface().(framework.CmdParam)
	fUse, _ := framework.GetCmdFromFlag(cp)
	if len(use) == 0 {
		use = fUse
	}

	if len(use) == 0 {
		fnName := mt.Name
		use = strings.ToLower(fnName[:len(fnName)-8])
	}
	uses := framework.ParseUseSegments(use)
	lastKw := uses[len(uses)-1]
	// hard code, show xxx command only
	if uses[0] != "show" {
		return
	}

	// fmt.Printf("path: /%s\n", lastKw)

	r.GET(fmt.Sprintf("/%s", lastKw), func(c *gin.Context) {

		info := &InstanceInfo{}
		c.ShouldBind(info)

		start := states.Start(app.config)
		s, err := start.Process(fmt.Sprintf("connect --etcd=%s --rootPath=%s", info.EtcdAddr, info.RootPath))

		if err != nil {
			c.Error(err)
			return
		}
		s.SetupCommands()

		v := reflect.ValueOf(s)
		cp := reflect.New(paramType.Elem()).Interface().(framework.CmdParam)
		setupDefaultValue(cp)
		if err := app.BindCmdParam(c, cp); err != nil {
			c.Error(err)
			return
		}

		m := v.MethodByName(mt.Name)
		results := m.Call([]reflect.Value{
			reflect.ValueOf(c),
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
				c.Error(err)
				return
			case result.Type().Implements(reflect.TypeOf((*framework.ResultSet)(nil)).Elem()):
				if result.IsNil() {
					continue
				}
				rs := result.Interface().(framework.ResultSet)
				c.JSON(http.StatusOK, rs.Entities())
				return
			}
		}

		c.Error(errors.New("unexpected branch reached, no result set found"))
	})
}

func (app *WebServerApp) BindCmdParam(c *gin.Context, cp framework.CmdParam) error {
	v := reflect.ValueOf(cp)
	if v.Kind() != reflect.Pointer {
		return errors.New("param is not pointer")
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
		rawStr, ok := c.GetQuery(name)
		if !ok {
			continue
		}
		switch f.Type.Kind() {
		case reflect.Int64:
			var dv int64
			if v, err := strconv.ParseInt(rawStr, 10, 64); err == nil {
				dv = v
			}
			v.Field(i).SetInt(dv)
			fmt.Println("set default", f.Name, dv)
		case reflect.String:
			v.Field(i).SetString(rawStr)
		case reflect.Bool:
			var dv bool
			if v, err := strconv.ParseBool(rawStr); err == nil {
				dv = v
			}
			v.Field(i).SetBool(dv)
		case reflect.Struct:
			continue
		default:
			return fmt.Errorf("field %s with kind %s not supported yet", f.Name, f.Type.Kind())
		}
	}
	return nil
}

func setupDefaultValue(p framework.CmdParam) {
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
		defaultStr := f.Tag.Get("default")
		switch f.Type.Kind() {
		case reflect.Int64:
			var dv int64
			if v, err := strconv.ParseInt(defaultStr, 10, 64); err == nil {
				dv = v
			}
			v.Field(i).SetInt(dv)
			fmt.Println("set default", f.Name, dv)
		case reflect.String:
			v.Field(i).SetString(defaultStr)
		case reflect.Bool:
			var dv bool
			if v, err := strconv.ParseBool(defaultStr); err == nil {
				dv = v
			}
			v.Field(i).SetBool(dv)
		case reflect.Struct:
			continue
		default:
			fmt.Printf("field %s with kind %s not supported yet\n", f.Name, f.Type.Kind())
		}
	}
}

func NewWebServerApp(port int, config *configs.Config) *WebServerApp {
	return &WebServerApp{
		port:   port,
		config: config,
	}
}
