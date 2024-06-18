package show

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/errors"

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
func (c *ComponentShow) ChannelWatchedCommand(ctx context.Context, p *ChannelWatchedParam) (*ChannelsWatched, error) {
	infos, err := common.ListChannelWatch(ctx, c.client, c.basePath, etcdversion.GetVersion(), func(channel *models.ChannelWatch) bool {
		return p.CollectionID == 0 || channel.Vchan.CollectionID == p.CollectionID
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to list channel watch info")
	}

	return framework.NewListResult[ChannelsWatched](infos), nil
}

type ChannelsWatched struct {
	framework.ListResultSet[*models.ChannelWatch]
}

func (rs *ChannelsWatched) PrintAs(format framework.Format) string {
	switch format {
	case framework.FormatDefault, framework.FormatPlain:
		sb := &strings.Builder{}
		for _, info := range rs.Data {
			rs.printChannelWatchInfo(sb, info)
		}

		fmt.Fprintf(sb, "--- Total Channels: %d\n", len(rs.Data))
		return sb.String()
	default:
	}
	return ""
}

func (rs *ChannelsWatched) printChannelWatchInfo(sb *strings.Builder, info *models.ChannelWatch) {
	fmt.Fprintln(sb, "=============================")
	fmt.Fprintf(sb, "key: %s\n", info.Key())
	fmt.Fprintf(sb, "Channel Name:%s \t WatchState: %s\n", info.Vchan.ChannelName, info.State.String())
	// t, _ := ParseTS(uint64(info.GetStartTs()))
	// to, _ := ParseTS(uint64(info.GetTimeoutTs()))
	t := time.Unix(info.StartTs, 0)
	to := time.Unix(0, info.TimeoutTs)
	fmt.Fprintf(sb, "Channel Watch start from: %s, timeout at: %s\n", t.Format(tsPrintFormat), to.Format(tsPrintFormat))

	pos := info.Vchan.SeekPosition
	if pos != nil {
		startTime, _ := utils.ParseTS(pos.Timestamp)
		fmt.Fprintf(sb, "Start Position ID: %v, time: %s\n", pos.MsgID, startTime.Format(tsPrintFormat))
	}

	fmt.Fprintf(sb, "Unflushed segments: %v\n", info.Vchan.UnflushedSegmentIds)
	fmt.Fprintf(sb, "Flushed segments: %v\n", info.Vchan.FlushedSegmentIds)
	fmt.Fprintf(sb, "Dropped segments: %v\n", info.Vchan.DroppedSegmentIds)
}
