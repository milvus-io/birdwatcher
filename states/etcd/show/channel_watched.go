package show

import (
	"context"
	"fmt"
	"time"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
	"github.com/milvus-io/birdwatcher/utils"
)

type ChannelWatchedParam struct {
	framework.ParamBase `use:"show channel-watch" desc:"display channel watching info from data coord meta store" alias:"channel-watched"`
	CollectionID        int64 `name:"collection" default:"0" desc:"collection id to filter with"`
}

// ChannelWatchedCommand return show channel-watched commands.
func (c *ComponentShow) ChannelWatchedCommand(ctx context.Context, p *ChannelWatchedParam) {
	infos, err := common.ListChannelWatch(ctx, c.client, c.basePath, etcdversion.GetVersion(), func(channel *models.ChannelWatch) bool {
		return p.CollectionID == 0 || channel.Vchan.CollectionID == p.CollectionID
	})
	if err != nil {
		fmt.Println("failed to list channel watch info", err.Error())
		return
	}

	for _, info := range infos {
		printChannelWatchInfo(info)
	}

	fmt.Printf("--- Total Channels: %d\n", len(infos))
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
