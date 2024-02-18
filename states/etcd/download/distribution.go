package download

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"time"

	"github.com/milvus-io/birdwatcher/models"
	commonpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/commonpb"
	querypbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/querypb"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
	"github.com/samber/lo"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func PullGlobalDistributionDetails(cli clientv3.KV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "global-distribution",
		Short: "pull global distribution details",
		RunE: func(cmd *cobra.Command, args []string) error {
			sessions, err := common.ListSessions(cli, basePath)
			if err != nil {
				return err
			}

			fileName, err := cmd.Flags().GetString("file")
			if err != nil {
				return err
			}
			f, err := os.Create(fileName)
			if err != nil {
				return err
			}

			defer f.Close()

			w := csv.NewWriter(f)
			defer w.Flush()

			for _, session := range sessions {
				opts := []grpc.DialOption{
					grpc.WithTransportCredentials(insecure.NewCredentials()),
					grpc.WithBlock(),
				}

				dialCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				conn, err := grpc.DialContext(dialCtx, session.Address, opts...)
				cancel()
				if err != nil {
					fmt.Printf("failed to connect %s(%d), err: %s\n", session.ServerName, session.ServerID, err.Error())
					continue
				}

				if session.ServerName == "querynode" {
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

					segmentIDs := make([]int64, 0)
					for _, lv := range resp.GetLeaderViews() {
						growings := lo.Uniq(lo.Union(lv.GetGrowingSegmentIDs(), lo.Keys(lv.GetGrowingSegments())))
						segmentIDs = append(segmentIDs, growings...)
					}

					for _, segment := range resp.GetSegments() {
						segmentIDs = append(segmentIDs, segment.ID)
					}

					segmentMap := lo.SliceToMap(segmentIDs, func(id int64) (int64, struct{}) {
						return id, struct{}{}
					})
					segments, err := common.ListSegmentsVersion(context.Background(), cli, basePath, etcdversion.GetVersion(), func(segment *models.Segment) bool {
						_, ok := segmentMap[segment.ID]
						return ok
					})
					if err != nil {
						return err
					}

					for _, segment := range segments {
						// serverID,collectionID,partitionID,segmentID,channelName,rowNum,state
						content := make([]string, 0)
						content = append(content, fmt.Sprintf("%d", session.ServerID))
						content = append(content, fmt.Sprintf("%d", segment.CollectionID))
						content = append(content, fmt.Sprintf("%d", segment.PartitionID))
						content = append(content, fmt.Sprintf("%d", segment.ID))
						content = append(content, segment.InsertChannel)
						content = append(content, fmt.Sprintf("%d", segment.NumOfRows))
						content = append(content, segment.State.String())
						w.Write(content)
					}
				}
			}
			return nil
		},
	}

	cmd.Flags().String("file", "distribution.csv", "file to save distribution details")
	return cmd
}
