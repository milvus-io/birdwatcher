package states

import (
	"context"
	"fmt"
	"time"

	commonpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/commonpb"
	querypbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/querypb"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

func GetDistributionCommand(cli clientv3.KV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "segment-loaded-grpc",
		Short: "list segments loaded infomration",
		RunE: func(cmd *cobra.Command, args []string) error {
			sessions, err := common.ListSessions(cli, basePath)
			if err != nil {
				return err
			}

			for _, session := range sessions {
				opts := []grpc.DialOption{
					grpc.WithInsecure(),
					grpc.WithBlock(),
					grpc.WithTimeout(2 * time.Second),
				}

				conn, err := grpc.DialContext(context.Background(), session.Address, opts...)
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
						return err
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

				}
			}

			return nil
		},
	}
	return cmd
}
