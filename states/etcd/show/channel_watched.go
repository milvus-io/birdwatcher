package show

import (
	"context"
	"fmt"
	"sort"
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

	fmt.Fprintf(sb, "Fields:\n")
	if info.Schema == nil {
		fmt.Fprintf(sb, "### Collection schema is empty!!! ###\n")
		return
	}
	fields := info.Schema.Fields
	sort.Slice(fields, func(i, j int) bool {
		return fields[i].FieldID < fields[j].FieldID
	})
	for _, field := range fields {
		fmt.Fprintf(sb, " - Field ID: %d \t Field Name: %s \t Field Type: %s\n", field.FieldID, field.Name, field.DataType.String())
		if field.IsPrimaryKey {
			fmt.Fprintf(sb, "\t - Primary Key: %t, AutoID: %t\n", field.IsPrimaryKey, field.AutoID)
		}
		if field.IsDynamic {
			fmt.Fprintf(sb, "\t - Dynamic Field\n")
		}
		if field.IsPartitionKey {
			fmt.Fprintf(sb, "\t - Partition Key\n")
		}
		if field.IsClusteringKey {
			fmt.Fprintf(sb, "\t - Clustering Key\n")
		}
		// print element type if field is array
		if field.DataType == models.DataTypeArray {
			fmt.Fprintf(sb, "\t - Element Type:  %s\n", field.ElementType.String())
		}
		// type params
		for key, value := range field.Properties {
			fmt.Fprintf(sb, "\t - Type Param %s: %s\n", key, value)
		}
	}

	fmt.Fprintf(sb, "Enable Dynamic Schema: %t\n", info.Schema.EnableDynamicSchema)
}
