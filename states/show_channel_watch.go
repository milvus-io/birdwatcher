package states

import (
	"context"
	"fmt"
	"path"
	"time"

	"github.com/milvus-io/birdwatcher/proto/v2.0/datapb"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func getEtcdShowChannelWatch(cli *clientv3.Client, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "channel-watch",
		Short:   "display channel watching info from data coord meta store",
		Aliases: []string{"channel-watched"},
		Run: func(cmd *cobra.Command, args []string) {

			collID, err := cmd.Flags().GetInt64("collection")
			if err != nil {
				return
			}

			infos, _, err := listChannelWatchV1(cli, basePath)
			if err != nil {
				fmt.Println("failed to list channel watch info", err.Error())
				return
			}

			for _, info := range infos {
				if collID > 0 && info.GetVchan().GetCollectionID() != collID {
					continue
				}

				printChannelWatchInfo(info)
			}

			fmt.Printf("--- Total Channels: %d\n", len(infos))
		},
	}
	cmd.Flags().Int64("collection", 0, "collection id to filter with")
	return cmd
}

func listChannelWatchV1(cli *clientv3.Client, basePath string) ([]datapb.ChannelWatchInfo, []string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	prefix := path.Join(basePath, "channelwatch") + "/"
	return listObject[datapb.ChannelWatchInfo](ctx, cli, prefix)
}

func printChannelWatchInfo(info datapb.ChannelWatchInfo) {
	fmt.Println("=============================")
	fmt.Printf("Channel Name:%s \t WatchState: %s\n", info.GetVchan().GetChannelName(), info.GetState().String())
	//t, _ := ParseTS(uint64(info.GetStartTs()))
	//to, _ := ParseTS(uint64(info.GetTimeoutTs()))
	t := time.Unix(info.GetStartTs(), 0)
	to := time.Unix(0, info.GetTimeoutTs())
	fmt.Printf("Channel Watch start from: %s, timeout at: %s\n", t.Format(tsPrintFormat), to.Format(tsPrintFormat))

	pos := info.GetVchan().GetSeekPosition()
	startTime, _ := ParseTS(pos.GetTimestamp())
	fmt.Printf("Start Position ID: %v, time: %s\n", pos.GetMsgID(), startTime.Format(tsPrintFormat))

	fmt.Printf("Unflushed segments: %v\n", info.Vchan.GetUnflushedSegmentIds())
	fmt.Printf("Flushed segments: %v\n", info.Vchan.GetFlushedSegmentIds())
	fmt.Printf("Dropped segments: %v\n", info.Vchan.GetDroppedSegmentIds())
}
