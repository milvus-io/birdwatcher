package states

import (
	"fmt"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"github.com/milvus-io/birdwatcher/common"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/proto/v2.0/rootcoordpb"
	rootcoordpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/rootcoordpb"
)

type rootCoordState struct {
	common.CmdState
	session   *models.Session
	client    rootcoordpb.RootCoordClient
	clientv2  rootcoordpbv2.RootCoordClient
	conn      *grpc.ClientConn
	prevState common.State
}

// SetupCommands setups the command.
// also called after each command run to reset flag values.
func (s *rootCoordState) SetupCommands() {
	cmd := &cobra.Command{}
	cmd.AddCommand(
		// metrics
		getMetricsCmd(s.client),
		// configuration
		getConfigurationCmd(s.clientv2, s.session.ServerID),
		// back
		getBackCmd(s, s.prevState),
		// exit
		getExitCmd(s),
	)
	s.MergeFunctionCommands(cmd, s)

	s.CmdState.RootCmd = cmd
	s.SetupFn = s.SetupCommands
}

func getRootCoordState(client rootcoordpb.RootCoordClient, conn *grpc.ClientConn, prev common.State, session *models.Session) common.State {
	state := &rootCoordState{
		session: session,
		CmdState: common.CmdState{
			LabelStr: fmt.Sprintf("RootCoord-%d(%s)", session.ServerID, session.Address),
		},
		client:    client,
		clientv2:  rootcoordpbv2.NewRootCoordClient(conn),
		conn:      conn,
		prevState: prev,
	}

	state.SetupCommands()

	return state
}
