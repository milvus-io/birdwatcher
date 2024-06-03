package repair

import (
	"context"
	"fmt"
	"time"

	"github.com/milvus-io/birdwatcher/proto/v2.0/commonpb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/datapb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/milvuspb"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

func ManualCompactionCommand(cli clientv3.KV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "manual-compaction",
		Short: "do manual compaction",
		Run: func(cmd *cobra.Command, args []string) {
			collID, err := cmd.Flags().GetInt64("collection")
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			if collID == 0 {
				fmt.Printf("collection id should not be zero\n")
				return
			}
			doManualCompaction(cli, basePath, collID)
		},
	}
	cmd.Flags().Int64("collection", 0, "collection id")
	return cmd
}

func doManualCompaction(cli clientv3.KV, basePath string, collID int64) {
	sessions, err := common.ListSessions(cli, basePath)
	if err != nil {
		fmt.Println("failed to list session")
		return
	}
	for _, session := range sessions {
		if session.ServerName == "datacoord" {
			opts := []grpc.DialOption{
				grpc.WithInsecure(),
				grpc.WithBlock(),
				grpc.WithTimeout(2 * time.Second),
			}

			conn, err := grpc.DialContext(context.Background(), session.Address, opts...)
			if err != nil {
				fmt.Printf("failed to connect to DataCoord(%d) addr: %s, err: %s\n", session.ServerID, session.Address, err.Error())
				return
			}

			client := datapb.NewDataCoordClient(conn)
			result, err := client.ManualCompaction(context.Background(), &milvuspb.ManualCompactionRequest{
				CollectionID: collID,
			})
			if err != nil {
				fmt.Println("failed to call ManualCompaction", err.Error())
				return
			}
			if result.Status.ErrorCode != commonpb.ErrorCode_Success {
				fmt.Printf("ManualCompaction failed, error code = %s, reason = %s\n", result.Status.ErrorCode.String(), result.Status.Reason)
				return
			}
			fmt.Printf("ManualCompaction trigger success, id: %d\n", result.CompactionID)
		}
	}
}
