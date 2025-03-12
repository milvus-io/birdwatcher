package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"

	_ "github.com/milvus-io/birdwatcher/asap"
	"github.com/milvus-io/birdwatcher/bapps"
	"github.com/milvus-io/birdwatcher/common"
	"github.com/milvus-io/birdwatcher/configs"
	"github.com/milvus-io/birdwatcher/states"
)

var (
	oneLineCommand = flag.String("olc", "", "one line command execution mode")
	simple         = flag.Bool("simple", false, "use simple ui without suggestion and history")
	restServer     = flag.Bool("rest", false, "rest server address")
	webPort        = flag.Int("port", 8002, "listening port for web server")
	printVersion   = flag.Bool("version", false, "print version")
	multiState     = flag.Bool("multiState", false, "use multi state feature, default false")
)

func main() {
	flag.Parse()

	var appFactory func(config *configs.Config) bapps.BApp

	switch {
	// Print current birdwatcher version
	case *printVersion:
		fmt.Println("Birdwatcher Version", common.Version)
		return
	case *simple:
		appFactory = func(*configs.Config) bapps.BApp { return bapps.NewSimpleApp() }
	case len(*oneLineCommand) > 0:
		appFactory = func(*configs.Config) bapps.BApp { return bapps.NewOlcApp(*oneLineCommand) }
	case *restServer:
		appFactory = func(config *configs.Config) bapps.BApp { return bapps.NewWebServerApp(*webPort, config) }
	default:
		defer handleExit()
		// open file and create if non-existent
		file, err := os.OpenFile("bw_debug.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()

		logger := log.New(file, "[Custom Log]", log.LstdFlags)

		appFactory = func(config *configs.Config) bapps.BApp {
			return bapps.NewPromptApp(config, bapps.WithLogger(logger), bapps.WithMultiStage(*multiState))
		}
	}

	config, err := configs.NewConfig(".bw_config")
	if err != nil {
		// run by default, just printing warning.
		fmt.Println("[WARN] load config file failed, running in default setting", err.Error())
	}

	start := states.Start(config, *multiState)

	app := appFactory(config)
	app.Run(start)
}

// handleExit is the fix for go-prompt output hi-jack fix.
func handleExit() {
	rawModeOff := exec.Command("/bin/stty", "-raw", "echo")
	rawModeOff.Stdin = os.Stdin
	_ = rawModeOff.Run()
	rawModeOff.Wait()
}
