package states

import (
	"context"
	"fmt"
	"strconv"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/proto/v2.0/commonpb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/milvuspb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/querypb"
	commonpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/commonpb"
	querypbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/querypb"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

type queryNodeState struct {
	cmdState
	session   *models.Session
	client    querypb.QueryNodeClient
	clientv2  querypbv2.QueryNodeClient
	conn      *grpc.ClientConn
	prevState State
}

// SetupCommands setups the command.
// also called after each command run to reset flag values.
func (s *queryNodeState) SetupCommands() {
	cmd := &cobra.Command{}
	cmd.AddCommand(
		// GetSegmentInfo collection_id
		getQNGetSegmentsCmd(s.client),
		// GetMetrics
		getQNGetMetrics(s.client),

		getQNGetDataDistributionCmd(s.clientv2, s.session.ServerID),

		getBackCmd(s, s.prevState),
		// exit
		getExitCmd(s),
	)
	cmd.AddCommand(getGlobalUtilCommands()...)

	s.cmdState.rootCmd = cmd
	s.setupFn = s.SetupCommands
}

func getQueryNodeState(client querypb.QueryNodeClient, conn *grpc.ClientConn, prev State, session *models.Session) State {

	state := &queryNodeState{
		session: session,
		cmdState: cmdState{
			label: fmt.Sprintf("QueryNode-%d(%s)", session.ServerID, session.Address),
		},
		client:    client,
		clientv2:  querypbv2.NewQueryNodeClient(conn),
		conn:      conn,
		prevState: prev,
	}

	state.SetupCommands()

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

func getQNGetDataDistributionCmd(clientv2 querypbv2.QueryNodeClient, id int64) *cobra.Command {
	cmd := &cobra.Command{
		Use: "GetDataDistribution",
		Run: func(cmd *cobra.Command, args []string) {
			resp, err := clientv2.GetDataDistribution(context.Background(), &querypbv2.GetDataDistributionRequest{
				Base: &commonpbv2.MsgBase{
					SourceID: -1,
					TargetID: id,
				},
			})
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			fmt.Println(resp.String())
		},
	}
	return cmd
}
