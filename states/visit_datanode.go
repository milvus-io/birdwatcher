package states

import (
	"context"
	"fmt"

	"github.com/congqixia/birdwatcher/models"
	"github.com/congqixia/birdwatcher/proto/v2.0/datapb"
	"github.com/congqixia/birdwatcher/proto/v2.0/milvuspb"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

type dataNodeState struct {
	cmdState
	client    datapb.DataNodeClient
	conn      *grpc.ClientConn
	prevState State
}

func getDataNodeState(client datapb.DataNodeClient, conn *grpc.ClientConn, prev State, session *models.Session) State {
	cmd := &cobra.Command{}

	state := &dataNodeState{
		cmdState: cmdState{
			label:   fmt.Sprintf("DataNode-%d(%s)", session.ServerID, session.Address),
			rootCmd: cmd,
		},
		client: client,
		conn:   conn,
	}

	cmd.AddCommand(
		//GetMetrics
		getDataNodeMetrics(client),
		//back
		getBackCmd(state, prev),
		// exit
		getExitCmd(state),
	)
	return state
}

func getDataNodeMetrics(client datapb.DataNodeClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "GetMetrics",
		Short: "show the metrics provided by this datanode",
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
