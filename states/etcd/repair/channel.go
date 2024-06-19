package repair

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"github.com/milvus-io/birdwatcher/proto/v2.0/commonpb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/datapb"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
	"github.com/milvus-io/birdwatcher/states/kv"
)

// ChannelCommand returns repair channel command.
func ChannelCommand(cli kv.MetaKV, basePath string) *cobra.Command {
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

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			coll, err := common.GetCollectionByIDVersion(ctx, cli, basePath, etcdversion.GetVersion(), collID)
			if err != nil {
				fmt.Println("collection not found")
				return
			}

			chans := make(map[string]struct{})
			for _, c := range coll.Channels {
				chans[c.VirtualName] = struct{}{}
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

func doDatacoordWatch(cli kv.MetaKV, basePath string, collectionID int64, vchannels []string) {
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
