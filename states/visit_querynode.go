package states

import (
	"context"
	"fmt"
	"strconv"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/proto/v2.0/commonpb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/querypb"
	commonpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/commonpb"
	querypbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/querypb"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

type queryNodeState struct {
	*framework.CmdState
	session   *models.Session
	client    querypb.QueryNodeClient
	clientv2  querypbv2.QueryNodeClient
	conn      *grpc.ClientConn
	prevState framework.State
}

// SetupCommands setups the command.
// also called after each command run to reset flag values.
func (s *queryNodeState) SetupCommands() {
	cmd := &cobra.Command{}
	cmd.AddCommand(
		// GetSegmentInfo collection_id
		getQNGetSegmentsCmd(s.client),
		// metrics
		getMetricsCmd(s.client),
		// configuration
		getConfigurationCmd(s.clientv2, s.session.ServerID),
		// distribution
		getQNGetDataDistributionCmd(s.clientv2, s.session.ServerID),
		// segment-analysis --collection collection --segment
		getQNGetSegmentInfoCmd(s.clientv2, s.session.ServerID),
		// back
		getBackCmd(s, s.prevState),
		// exit
		getExitCmd(s),
	)

	s.MergeFunctionCommands(cmd, s)

	s.CmdState.RootCmd = cmd
	s.SetupFn = s.SetupCommands
}

func getQueryNodeState(client querypb.QueryNodeClient, conn *grpc.ClientConn, prev framework.State, session *models.Session) framework.State {

	state := &queryNodeState{
		CmdState:  framework.NewCmdState(fmt.Sprintf("QueryNode-%d(%s)", session.ServerID, session.Address)),
		session:   session,
		client:    client,
		clientv2:  querypbv2.NewQueryNodeClient(conn),
		conn:      conn,
		prevState: prev,
	}

	state.SetupCommands()

	return state
}

func getBackCmd(state, prev framework.State) *cobra.Command {
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

func getQNGetDataDistributionCmd(clientv2 querypbv2.QueryNodeClient, id int64) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "distribution",
		Short:   "get data distribution",
		Aliases: []string{"GetDataDistribution"},
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

			// print channel
			for _, channel := range resp.GetChannels() {
				fmt.Printf("Channel %s, collection: %d, version %d\n", channel.Channel, channel.Collection, channel.Version)
			}

			for _, lv := range resp.GetLeaderViews() {
				fmt.Printf("Leader view for channel: %s\n", lv.GetChannel())
				growings := lv.GetGrowingSegmentIDs()
				fmt.Printf("Growing segments number: %d , ids: %v\n", len(growings), growings)
			}

			fmt.Printf("Node Loaded Segments number: %d\n", len(resp.GetSegments()))
		},
	}
	return cmd
}

func getQNGetSegmentInfoCmd(clientv2 querypbv2.QueryNodeClient, id int64) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "segment-analysis",
		Short:   "call ShowConfigurations for config inspection",
		Aliases: []string{"GetConfigurations", "configurations"},
		Run: func(cmd *cobra.Command, args []string) {
			segmentID, err := cmd.Flags().GetInt64("segment")
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			collectionID, err := cmd.Flags().GetInt64("collection")
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			printBrute, err := cmd.Flags().GetBool("print")
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			if segmentID == 0 && collectionID == 0 {
				fmt.Println("segment id & collection id not provided")
				return
			}
			req := &querypbv2.GetSegmentInfoRequest{
				Base: &commonpbv2.MsgBase{
					SourceID: -1,
					TargetID: id,
				},
				CollectionID: collectionID,
			}
			if segmentID > 0 {
				req.SegmentIDs = []int64{segmentID}
			}

			resp, err := clientv2.GetSegmentInfo(context.Background(), req)
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			var growing, indexed, bruteForce int64
		outer:
			for _, info := range resp.GetInfos() {
				if len(info.GetIndexInfos()) == 0 {
					fmt.Printf("Brute force segment found: %d , state: %s\n", info.GetSegmentID(), info.GetSegmentState().String())
					if printBrute {
						fmt.Printf("%#v\n", info.String())
					}
					bruteForce++
					continue
				}
				for _, idx := range info.GetIndexInfos() {
					if len(idx.GetIndexFilePaths()) == 0 {
						fmt.Printf("Brute force segment found: %d , state: %s, empty info: %+v\n", info.GetSegmentID(), info.GetSegmentState().String(), idx)
						if printBrute {
							fmt.Printf("%#v\n", info.String())
						}
						bruteForce++
						continue outer
					}
				}
				switch info.SegmentState {
				case commonpbv2.SegmentState_Sealed:
					indexed++
				case commonpbv2.SegmentState_Growing:
					growing++
				}
			}
			fmt.Printf("Indexed :%d BruteForce: %d, Growing: %d\n", indexed, bruteForce, growing)
		},
	}

	cmd.Flags().Int64("segment", 0, "segment id")
	cmd.Flags().Int64("collection", 0, "collection id")
	cmd.Flags().Bool("print", false, "print brute force segment info")
	return cmd
}
