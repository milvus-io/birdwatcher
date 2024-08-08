package states

import (
	"fmt"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/proto/v2.0/datapb"
	datapbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/datapb"
)

type dataCoordState struct {
	cmdState
	session   *models.Session
	client    datapb.DataCoordClient
	clientv2  datapbv2.DataCoordClient
	conn      *grpc.ClientConn
	prevState State
}

// SetupCommands setups the command.
// also called after each command run to reset flag values.
func (s *dataCoordState) SetupCommands() {
	cmd := &cobra.Command{}
	cmd.AddCommand(
		// metrics
		getMetricsCmd(s.client),
		// configuration
		getConfigurationCmd(s.clientv2, s.session.ServerID),
		// back
		getBackCmd(s, s.prevState),

		// compact
		compactCmd(s.clientv2),

		// exit
		getExitCmd(s),
	)

	s.mergeFunctionCommands(cmd, s)

	s.cmdState.rootCmd = cmd
	s.setupFn = s.SetupCommands
}

func getDataCoordState(client datapb.DataCoordClient, conn *grpc.ClientConn, prev State, session *models.Session) State {
	state := &dataCoordState{
		cmdState: cmdState{
			label: fmt.Sprintf("DataCoord-%d(%s)", session.ServerID, session.Address),
		},
		session:   session,
		client:    client,
		clientv2:  datapbv2.NewDataCoordClient(conn),
		conn:      conn,
		prevState: prev,
	}

	state.SetupCommands()

	return state
}
