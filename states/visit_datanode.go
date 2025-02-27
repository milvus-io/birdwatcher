package states

import (
	"fmt"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

type dataNodeState struct {
	*framework.CmdState
	session   *models.Session
	client    datapb.DataNodeClient
	conn      *grpc.ClientConn
	prevState framework.State
}

// SetupCommands setups the command.
// also called after each command run to reset flag values.
func (s *dataNodeState) SetupCommands() {
	cmd := &cobra.Command{}
	cmd.AddCommand(
		// metrics
		getMetricsCmd(s.client),
		// configuration
		getConfigurationCmd(s.client, s.session.ServerID),
		// back
		getBackCmd(s, s.prevState),
		// exit
		getExitCmd(s),
	)

	s.MergeFunctionCommands(cmd, s)

	s.CmdState.RootCmd = cmd
	s.SetupFn = s.SetupCommands
}

func getDataNodeState(client datapb.DataNodeClient, conn *grpc.ClientConn, prev framework.State, session *models.Session) framework.State {
	state := &dataNodeState{
		CmdState:  framework.NewCmdState(fmt.Sprintf("DataNode-%d(%s)", session.ServerID, session.Address)),
		session:   session,
		client:    client,
		conn:      conn,
		prevState: prev,
	}

	state.SetupCommands()

	return state
}
