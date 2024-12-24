package states

import (
	"fmt"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"github.com/milvus-io/birdwatcher/common"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/proto/v2.0/datapb"
	datapbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/datapb"
)

type dataNodeState struct {
	common.CmdState
	session   *models.Session
	client    datapb.DataNodeClient
	clientv2  datapbv2.DataNodeClient
	conn      *grpc.ClientConn
	prevState common.State
}

// SetupCommands setups the command.
// also called after each command run to reset flag values.
func (s *dataNodeState) SetupCommands() {
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

func getDataNodeState(client datapb.DataNodeClient, conn *grpc.ClientConn, prev common.State, session *models.Session) common.State {
	state := &dataNodeState{
		CmdState: common.CmdState{
			LabelStr: fmt.Sprintf("DataNode-%d(%s)", session.ServerID, session.Address),
		},
		session:   session,
		client:    client,
		clientv2:  datapbv2.NewDataNodeClient(conn),
		conn:      conn,
		prevState: prev,
	}

	state.SetupCommands()

	return state
}
