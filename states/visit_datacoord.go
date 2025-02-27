package states

import (
	"fmt"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

type dataCoordState struct {
	*framework.CmdState
	session   *models.Session
	client    datapb.DataCoordClient
	conn      *grpc.ClientConn
	prevState framework.State
}

// SetupCommands setups the command.
// also called after each command run to reset flag values.
func (s *dataCoordState) SetupCommands() {
	cmd := &cobra.Command{}
	cmd.AddCommand(
		// metrics
		getMetricsCmd(s.client),
		// configuration
		getConfigurationCmd(s.client, s.session.ServerID),
		// back
		getBackCmd(s, s.prevState),

		// compact
		compactCmd(s.client),

		// exit
		getExitCmd(s),
	)

	s.MergeFunctionCommands(cmd, s)

	s.CmdState.RootCmd = cmd
	s.SetupFn = s.SetupCommands
}

func getDataCoordState(client datapb.DataCoordClient, conn *grpc.ClientConn, prev framework.State, session *models.Session) framework.State {
	state := &dataCoordState{
		CmdState:  framework.NewCmdState(fmt.Sprintf("DataCoord-%d(%s)", session.ServerID, session.Address)),
		session:   session,
		client:    client,
		conn:      conn,
		prevState: prev,
	}

	state.SetupCommands()

	return state
}
