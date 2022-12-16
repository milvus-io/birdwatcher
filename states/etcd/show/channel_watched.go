package show

import (
	"fmt"
	"time"

	"github.com/milvus-io/birdwatcher/proto/v2.0/datapb"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/utils"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// ChannelWatchedCommand return show channel-watched commands.
func ChannelWatchedCommand(cli *clientv3.Client, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "channel-watch",
		Short:   "display channel watching info from data coord meta store",
		Aliases: []string{"channel-watched"},
		Run: func(cmd *cobra.Command, args []string) {

			collID, err := cmd.Flags().GetInt64("collection")
			if err != nil {
				return
			}

			infos, keys, err := common.ListChannelWatchV1(cli, basePath, func(channel *datapb.ChannelWatchInfo) bool {
				return collID == 0 || channel.GetVchan().GetCollectionID() == collID
			})
			if err != nil {
				fmt.Println("failed to list channel watch info", err.Error())
				return
			}

			for i, info := range infos {
				printChannelWatchInfo(info, keys[i])
			}

			fmt.Printf("--- Total Channels: %d\n", len(infos))
		},
	}
	cmd.Flags().Int64("collection", 0, "collection id to filter with")
	return cmd
}

func printChannelWatchInfo(info datapb.ChannelWatchInfo, key string) {
	fmt.Println("=============================")
	fmt.Printf("key: %s\n", key)
	fmt.Printf("Channel Name:%s \t WatchState: %s\n", info.GetVchan().GetChannelName(), info.GetState().String())
	//t, _ := ParseTS(uint64(info.GetStartTs()))
	//to, _ := ParseTS(uint64(info.GetTimeoutTs()))
	t := time.Unix(info.GetStartTs(), 0)
	to := time.Unix(0, info.GetTimeoutTs())
	fmt.Printf("Channel Watch start from: %s, timeout at: %s\n", t.Format(tsPrintFormat), to.Format(tsPrintFormat))

	pos := info.GetVchan().GetSeekPosition()
	startTime, _ := utils.ParseTS(pos.GetTimestamp())
	fmt.Printf("Start Position ID: %v, time: %s\n", pos.GetMsgID(), startTime.Format(tsPrintFormat))

	fmt.Printf("Unflushed segments: %v\n", info.Vchan.GetUnflushedSegmentIds())
	fmt.Printf("Flushed segments: %v\n", info.Vchan.GetFlushedSegmentIds())
	fmt.Printf("Dropped segments: %v\n", info.Vchan.GetDroppedSegmentIds())
}
