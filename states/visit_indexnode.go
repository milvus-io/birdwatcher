package states

import (
	"fmt"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/proto/v2.0/indexpb"
	indexpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/indexpb"
)

type indexNodeState struct {
	cmdState
	session   *models.Session
	client    indexpb.IndexNodeClient
	clientv2  indexpbv2.IndexNodeClient
	conn      *grpc.ClientConn
	prevState State
}

// SetupCommands setups the command.
// also called after each command run to reset flag values.
func (s *indexNodeState) SetupCommands() {
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
	s.mergeFunctionCommands(cmd, s)

	s.cmdState.rootCmd = cmd
	s.setupFn = s.SetupCommands
}

func getIndexNodeState(client indexpb.IndexNodeClient, conn *grpc.ClientConn, prev State, session *models.Session) State {
	state := &indexNodeState{
		cmdState: cmdState{
			label: fmt.Sprintf("IndexNode-%d(%s)", session.ServerID, session.Address),
		},
		session:   session,
		client:    client,
		clientv2:  indexpbv2.NewIndexNodeClient(conn),
		conn:      conn,
		prevState: prev,
	}

	state.SetupCommands()

	return state
}
