package show

import (
	"context"
	"fmt"
	"time"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
	"github.com/milvus-io/birdwatcher/utils"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// ChannelWatchedCommand return show channel-watched commands.
func ChannelWatchedCommand(cli clientv3.KV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "channel-watch",
		Short:   "display channel watching info from data coord meta store",
		Aliases: []string{"channel-watched"},
		Run: func(cmd *cobra.Command, args []string) {

			collID, err := cmd.Flags().GetInt64("collection")
			if err != nil {
				return
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			infos, err := common.ListChannelWatch(ctx, cli, basePath, etcdversion.GetVersion(), func(channel *models.ChannelWatch) bool {
				return collID == 0 || channel.Vchan.CollectionID == collID
			})
			if err != nil {
				fmt.Println("failed to list channel watch info", err.Error())
				return
			}

			for _, info := range infos {
				printChannelWatchInfo(info)
			}

			fmt.Printf("--- Total Channels: %d\n", len(infos))
		},
	}
	cmd.Flags().Int64("collection", 0, "collection id to filter with")
	return cmd
}

func printChannelWatchInfo(info *models.ChannelWatch) {
	fmt.Println("=============================")
	fmt.Printf("key: %s\n", info.Key())
	fmt.Printf("Channel Name:%s \t WatchState: %s\n", info.Vchan.ChannelName, info.State.String())
	//t, _ := ParseTS(uint64(info.GetStartTs()))
	//to, _ := ParseTS(uint64(info.GetTimeoutTs()))
	t := time.Unix(info.StartTs, 0)
	to := time.Unix(0, info.TimeoutTs)
	fmt.Printf("Channel Watch start from: %s, timeout at: %s\n", t.Format(tsPrintFormat), to.Format(tsPrintFormat))

	pos := info.Vchan.SeekPosition
	if pos != nil {
		startTime, _ := utils.ParseTS(pos.Timestamp)
		fmt.Printf("Start Position ID: %v, time: %s\n", pos.MsgID, startTime.Format(tsPrintFormat))
	}

	fmt.Printf("Unflushed segments: %v\n", info.Vchan.UnflushedSegmentIds)
	fmt.Printf("Flushed segments: %v\n", info.Vchan.FlushedSegmentIds)
	fmt.Printf("Dropped segments: %v\n", info.Vchan.DroppedSegmentIds)
}
