package mgrpc

import (
	"context"
	"fmt"
	"strconv"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
)

type queryNodeState struct {
	*framework.CmdState
	session   *models.Session
	client    querypb.QueryNodeClient
	conn      *grpc.ClientConn
	prevState framework.State
}

// SetupCommands setups the command.
// also called after each command run to reset flag values.
func (s *queryNodeState) SetupCommands() {
	cmd := s.GetCmd()
	cmd.AddCommand(
		// GetSegmentInfo collection_id
		getQNGetSegmentsCmd(s.client),
		// // metrics
		// getMetricsCmd(s.client),
		// // configuration
		// getConfigurationCmd(s.client, s.session.ServerID),
		// distribution
		getQNGetDataDistributionCmd(s.client, s.session.ServerID),
		// segment-analysis --collection collection --segment
		getQNGetSegmentInfoCmd(s.client, s.session.ServerID),
	)

	s.UpdateState(cmd, s, s.SetupCommands)
}

func GetQueryNodeState(client querypb.QueryNodeClient, conn *grpc.ClientConn, prev *framework.CmdState, session *models.Session) framework.State {
	state := &queryNodeState{
		CmdState:  prev.Spawn(fmt.Sprintf("QueryNode-%d(%s)", session.ServerID, session.Address)),
		session:   session,
		client:    client,
		conn:      conn,
		prevState: prev,
	}

	state.SetupCommands()

	return state
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

func getQNGetDataDistributionCmd(clientv2 querypb.QueryNodeClient, id int64) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "distribution",
		Short:   "get data distribution",
		Aliases: []string{"GetDataDistribution"},
		Run: func(cmd *cobra.Command, args []string) {
			resp, err := clientv2.GetDataDistribution(context.Background(), &querypb.GetDataDistributionRequest{
				Base: &commonpb.MsgBase{
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

func getQNGetSegmentInfoCmd(clientv2 querypb.QueryNodeClient, id int64) *cobra.Command {
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
			req := &querypb.GetSegmentInfoRequest{
				Base: &commonpb.MsgBase{
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
				case commonpb.SegmentState_Sealed:
					indexed++
				case commonpb.SegmentState_Growing:
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
