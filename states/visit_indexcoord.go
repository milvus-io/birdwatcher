package states

import (
	"context"
	"fmt"

	"github.com/congqixia/birdwatcher/models"
	"github.com/congqixia/birdwatcher/proto/v2.0/commonpb"
	"github.com/congqixia/birdwatcher/proto/v2.0/indexpb"
	"github.com/congqixia/birdwatcher/proto/v2.0/milvuspb"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

type indexCoordState struct {
	cmdState
	client    indexpb.IndexCoordClient
	conn      *grpc.ClientConn
	prevState State
}

func getIndexCoordState(client indexpb.IndexCoordClient, conn *grpc.ClientConn, prev State, session *models.Session) State {
	cmd := &cobra.Command{}

	state := &indexCoordState{
		cmdState: cmdState{
			label:   fmt.Sprintf("IndexCoord-%d(%s)", session.ServerID, session.Address),
			rootCmd: cmd,
		},
		client: client,
		conn:   conn,
	}

	cmd.AddCommand(
		//GetMetrics
		getIndexCoordMetrics(client),
		//back
		getBackCmd(state, prev),
		// exit
		getExitCmd(state),
	)
	return state
}

func getIndexCoordMetrics(client indexpb.IndexCoordClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "GetMetrics",
		Short: "show the metrics provided by this indexcoord",
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
