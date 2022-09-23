package states

import (
	"context"
	"fmt"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/proto/v2.0/commonpb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/milvuspb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/querypb"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

type queryCoordState struct {
	cmdState
	client    querypb.QueryCoordClient
	conn      *grpc.ClientConn
	prevState State
}

// SetupCommands setups the command.
// also called after each command run to reset flag values.
func (s *queryCoordState) SetupCommands() {
	cmd := &cobra.Command{}
	cmd.AddCommand(
		//GetMetrics
		getQueryCoordMetrics(s.client),
		//back
		getBackCmd(s, s.prevState),
		// exit
		getExitCmd(s),
	)
	cmd.AddCommand(getGlobalUtilCommands()...)

	s.cmdState.rootCmd = cmd
	s.setupFn = s.SetupCommands
}

func getQueryCoordState(client querypb.QueryCoordClient, conn *grpc.ClientConn, prev State, session *models.Session) State {

	state := &queryCoordState{
		cmdState: cmdState{
			label: fmt.Sprintf("QueryCoord-%d(%s)", session.ServerID, session.Address),
		},
		client:    client,
		conn:      conn,
		prevState: prev,
	}

	state.SetupCommands()

	return state
}

func getQueryCoordMetrics(client querypb.QueryCoordClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "GetMetrics",
		Short: "show the metrics provided by this querycoord",
		Run: func(cmd *cobra.Command, args []string) {

			resp, err := client.GetMetrics(context.Background(), &milvuspb.GetMetricsRequest{
				Base:    &commonpb.MsgBase{},
				Request: `{"metric_type": "system_info"}`,
			})
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			fmt.Printf("Metrics: %#v\n", resp.Response)
		},
	}
	return cmd
}
