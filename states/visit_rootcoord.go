package states

import (
	"context"
	"fmt"

	"github.com/congqixia/birdwatcher/models"
	"github.com/congqixia/birdwatcher/proto/v2.0/milvuspb"
	"github.com/congqixia/birdwatcher/proto/v2.0/rootcoordpb"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

type rootCoordState struct {
	cmdState
	client    rootcoordpb.RootCoordClient
	conn      *grpc.ClientConn
	prevState State
}

func getRootCoordState(client rootcoordpb.RootCoordClient, conn *grpc.ClientConn, prev State, session *models.Session) State {
	cmd := &cobra.Command{}

	state := &rootCoordState{
		cmdState: cmdState{
			label:   fmt.Sprintf("RootCoord-%d(%s)", session.ServerID, session.Address),
			rootCmd: cmd,
		},
		client: client,
		conn:   conn,
	}

	cmd.AddCommand(
		//GetMetrics
		getRootCoordMetrics(client),
		//back
		getBackCmd(state, prev),
		// exit
		getExitCmd(state),
	)
	return state
}

func getRootCoordMetrics(client rootcoordpb.RootCoordClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "GetMetrics",
		Short: "show the metrics provided by this rootcoord",
		Run: func(cmd *cobra.Command, args []string) {

			resp, err := client.GetMetrics(context.Background(), &milvuspb.GetMetricsRequest{
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
