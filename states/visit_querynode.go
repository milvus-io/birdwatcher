package states

import (
	"context"
	"fmt"
	"strconv"

	"github.com/congqixia/birdwatcher/models"
	"github.com/congqixia/birdwatcher/proto/v2.0/commonpb"
	"github.com/congqixia/birdwatcher/proto/v2.0/milvuspb"
	"github.com/congqixia/birdwatcher/proto/v2.0/querypb"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

type queryNodeState struct {
	cmdState
	client    querypb.QueryNodeClient
	conn      *grpc.ClientConn
	prevState State
}

func getQueryNodeState(client querypb.QueryNodeClient, conn *grpc.ClientConn, prev State, session *models.Session) State {
	cmd := &cobra.Command{}

	state := &queryNodeState{
		cmdState: cmdState{
			label:   fmt.Sprintf("QueryNode-%d(%s)", session.ServerID, session.Address),
			rootCmd: cmd,
		},
		client: client,
		conn:   conn,
	}

	cmd.AddCommand(
		// GetSegmentInfo collection_id
		getQNGetSegmentsCmd(client),
		// GetMetrics
		getQNGetMetrics(client),

		getBackCmd(state, prev),
		// exit
		getExitCmd(state),
	)
	return state
}

func getBackCmd(state, prev State) *cobra.Command {
	return &cobra.Command{
		Use: "back",
		Run: func(cmd *cobra.Command, args []string) {
			state.SetNext(prev)
		},
	}
}

func getQNGetSegmentsCmd(client querypb.QueryNodeClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "GetSegmentInfo collection_id",
		Short: "list loaded segments of provided collection in current querynode",
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) < 1 {
				cmd.Usage()
				return
			}

			collectionID, err := strconv.ParseInt(args[0], 10, 64)
			if err != nil {
				cmd.Usage()
				return
			}

			resp, err := client.GetSegmentInfo(context.Background(), &querypb.GetSegmentInfoRequest{
				Base:         &commonpb.MsgBase{},
				CollectionID: collectionID,
			})
			if err != nil {
				fmt.Println("failed to call grpc, err:", err.Error())
			}

			for _, info := range resp.GetInfos() {
				fmt.Printf("info: %#v\n", info)
			}
		},
	}

	return cmd
}

func getQNGetMetrics(client querypb.QueryNodeClient) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "GetMetrics",
		Short: "show the metrics provided by this querynode",
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
