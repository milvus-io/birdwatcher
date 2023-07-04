package states

import (
	"github.com/milvus-io/birdwatcher/configs"
	"github.com/spf13/cobra"
)

type disconnectState struct {
	cmdState
	config *configs.Config
}

func (s *disconnectState) SetupCommands() {
	cmd := &cobra.Command{}

	s.mergeFunctionCommands(cmd, s)

	s.rootCmd = cmd
	s.setupFn = s.SetupCommands
}

func getDisconnectedState(config *configs.Config) State {
	state := &disconnectState{
		cmdState: cmdState{
			label: "Offline",
		},
		config: config,
	}

	state.SetupCommands()
	return state
}
