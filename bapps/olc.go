package bapps

import (
	"fmt"
	"os"
	"strings"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/samber/lo"
)

type olcApp struct {
	script string
}

type olcCmd struct {
	cmd   string
	muted bool
}

func NewOlcApp(script string) BApp {
	return &olcApp{
		script: script,
	}
}

func (a *olcApp) Run(start framework.State) {
	app := start
	cmds := a.parseScripts(a.script)
	var err error
	for _, cmd := range cmds {
		stdout := os.Stdout
		if cmd.muted {
			// set to /dev/null to discard not wanted output
			os.Stdout, _ = os.Open(os.DevNull)
		}
		app, err = app.Process(cmd.cmd)
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		if cmd.muted {
			os.Stdout = stdout
		}
	}
}

func (a *olcApp) parseScripts(script string) []olcCmd {
	parts := strings.Split(script, ",")
	return lo.Map(parts, func(raw string, _ int) olcCmd {
		muted := false
		cmd := raw
		// mute cmd using #[command]
		if strings.HasPrefix(cmd, "#") {
			muted = true
			cmd = cmd[1:]
		}
		return olcCmd{
			muted: muted,
			cmd:   cmd,
		}
	})
}
