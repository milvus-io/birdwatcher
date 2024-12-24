package states

import (
	"github.com/spf13/cobra"

	"github.com/milvus-io/birdwatcher/common"
	"github.com/milvus-io/birdwatcher/configs"
)

type disconnectState struct {
	common.CmdState
	config *configs.Config
}

func (s *disconnectState) SetupCommands() {
	cmd := &cobra.Command{}

	s.MergeFunctionCommands(cmd, s)

	s.RootCmd = cmd
	s.SetupFn = s.SetupCommands
}

func getDisconnectedState(config *configs.Config) common.State {
	state := &disconnectState{
		CmdState: common.CmdState{
			LabelStr: "Offline",
		},
		config: config,
	}

	state.SetupCommands()
	return state
}
