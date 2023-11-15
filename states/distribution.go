package states

import (
	"context"
	"fmt"
	"time"

	commonpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/commonpb"
	querypbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/querypb"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// GetDistributionCommand returns command to iterate all querynodes to list distribution.
func GetDistributionCommand(cli kv.MetaKV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "segment-loaded-grpc",
		Short: "list segments loaded information",
		RunE: func(cmd *cobra.Command, args []string) error {
			collectionID, err := cmd.Flags().GetInt64("collection")
			var sealedCnt int64
			if err != nil {
				return err
			}
			sessions, err := common.ListSessions(cli, basePath)
			if err != nil {
				return err
			}

			for _, session := range sessions {
				opts := []grpc.DialOption{
					grpc.WithTransportCredentials(insecure.NewCredentials()),
					grpc.WithBlock(),
				}

				var conn *grpc.ClientConn
				var err error
				func() {
					ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
					defer cancel()

					conn, err = grpc.DialContext(ctx, session.Address, opts...)
				}()
				if err != nil {
					fmt.Printf("failed to connect %s(%d), err: %s\n", session.ServerName, session.ServerID, err.Error())
					continue
				}

				if session.ServerName == "querynode" {
					fmt.Println("===========")
					fmt.Printf("ServerID %d\n", session.ServerID)
					clientv2 := querypbv2.NewQueryNodeClient(conn)
					resp, err := clientv2.GetDataDistribution(context.Background(), &querypbv2.GetDataDistributionRequest{
						Base: &commonpbv2.MsgBase{
							SourceID: -1,
							TargetID: session.ServerID,
						},
					})
					if err != nil {
						fmt.Println(err.Error())
						continue
					}

					// print channel
					for _, channel := range resp.GetChannels() {
						if collectionID != 0 && channel.GetCollection() != collectionID {
							continue
						}
						fmt.Printf("Channel %s, collection: %d, version %d\n", channel.Channel, channel.Collection, channel.Version)
					}

					for _, lv := range resp.GetLeaderViews() {
						if collectionID != 0 && lv.GetCollection() != collectionID {
							continue
						}
						fmt.Printf("Leader view for channel: %s\n", lv.GetChannel())
						growings := lv.GetGrowingSegmentIDs()
						fmt.Printf("Growing segments number: %d , ids: %v\n", len(growings), growings)
					}

					sealedNum := 0
					for _, segment := range resp.GetSegments() {
						if collectionID != 0 && segment.GetCollection() != collectionID {
							continue
						}
						fmt.Printf("SegmentID: %d CollectionID: %d Channel: %s\n", segment.GetID(), segment.GetCollection(), segment.GetChannel())
						sealedNum++
					}
					fmt.Println("Sealed segments number:", sealedNum)
					sealedCnt += int64(sealedNum)
				}
			}
			fmt.Printf("==== total loaded sealed segment number: %d\n", sealedCnt)

			return nil
		},
	}
	cmd.Flags().Int64("collection", 0, "collection id to filter with")
	return cmd
}
