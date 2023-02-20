package repair

import (
	"context"
	"fmt"
	"time"

	"github.com/milvus-io/birdwatcher/proto/v2.0/commonpb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/datapb"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

// ChannelCommand returns repair channel command.
func ChannelCommand(cli clientv3.KV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "channel",
		Aliases: []string{"channels"},
		Short:   "do channel watch change and try to repair",
		Run: func(cmd *cobra.Command, args []string) {
			collID, err := cmd.Flags().GetInt64("collection")
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			run, err := cmd.Flags().GetBool("run")
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			if collID == 0 {
				fmt.Println("collection id not provided")
				return
			}

			coll, err := common.GetCollectionByID(cli, basePath, collID)
			if err != nil {
				fmt.Println("collection not found")
				return
			}

			chans := make(map[string]struct{})
			for _, vchan := range coll.GetVirtualChannelNames() {
				chans[vchan] = struct{}{}
			}

			infos, _, err := common.ListChannelWatchV1(cli, basePath)
			if err != nil {
				fmt.Println("failed to list channel watch info", err.Error())
				return
			}

			for _, info := range infos {
				delete(chans, info.GetVchan().GetChannelName())
			}

			for vchan := range chans {
				fmt.Println("orphan channel found", vchan)
			}
			if len(chans) == 0 {
				fmt.Println("no orphan channel found")
				return
			}
			if run {
				vchannelNames := make([]string, 0, len(chans))
				for vchan := range chans {
					vchannelNames = append(vchannelNames, vchan)
				}
				doDatacoordWatch(cli, basePath, collID, vchannelNames)
			}
		},
	}

	cmd.Flags().Int64("collection", 0, "collection id to filter with")
	cmd.Flags().Bool("run", false, "actual do repair")
	return cmd
}

func doDatacoordWatch(cli clientv3.KV, basePath string, collectionID int64, vchannels []string) {
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
			resp, err := client.WatchChannels(context.Background(), &datapb.WatchChannelsRequest{
				CollectionID: collectionID,
				ChannelNames: vchannels,
			})
			if err != nil {
				fmt.Println("failed to call WatchChannels", err.Error())
				return
			}

			if resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
				fmt.Println("WatchChannels failed", resp.GetStatus().GetErrorCode().String(), resp.GetStatus().GetReason())
				return
			}

			fmt.Printf("Invoke WatchChannels(%v) done\n", vchannels)
		}
	}
}
