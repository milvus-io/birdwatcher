package states

import (
	"github.com/spf13/cobra"

	"github.com/milvus-io/birdwatcher/configs"
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
