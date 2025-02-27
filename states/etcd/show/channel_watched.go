package show

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/utils"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

type ChannelWatchedParam struct {
	framework.ParamBase `use:"show channel-watch" desc:"display channel watching info from data coord meta store" alias:"channel-watched"`
	Format              string `name:"format" default:"" desc:"output format"`
	CollectionID        int64  `name:"collection" default:"0" desc:"collection id to filter with"`
	WithoutSchema       bool   `name:"withoutSchema" default:"false" desc:"filter channel watch info with not schema"`
	PrintSchema         bool   `name:"printSchema" default:"false" desc:"print schema info stored in watch info"`
}

// ChannelWatchedCommand return show channel-watched commands.
func (c *ComponentShow) ChannelWatchedCommand(ctx context.Context, p *ChannelWatchedParam) (*framework.PresetResultSet, error) {
	infos, err := common.ListChannelWatch(ctx, c.client, c.metaPath, func(ch *models.ChannelWatch) bool {
		channel := ch.GetProto()
		return (p.CollectionID == 0 || channel.GetVchan().CollectionID == p.CollectionID) && (!p.WithoutSchema || channel.GetSchema() == nil)
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to list channel watch info")
	}

	rs := framework.NewListResult[ChannelsWatched](infos)
	rs.printSchema = p.PrintSchema

	return framework.NewPresetResultSet(rs, framework.NameFormat(p.Format)), nil
}

type ChannelsWatched struct {
	framework.ListResultSet[*models.ChannelWatch]
	printSchema bool
}

func (rs *ChannelsWatched) PrintAs(format framework.Format) string {
	sb := &strings.Builder{}
	for _, info := range rs.Data {
		switch format {
		case framework.FormatDefault, framework.FormatPlain:
			rs.printChannelWatchInfo(sb, info)
		case framework.FormatJSON:
			rs.printChannelWatchInfoJSON(sb, info)
		default:
		}
	}

	switch format {
	case framework.FormatDefault, framework.FormatPlain:
		fmt.Fprintf(sb, "--- Total Channels: %d\n", len(rs.Data))
	default:
	}

	return sb.String()
}

func (rs *ChannelsWatched) printChannelWatchInfo(sb *strings.Builder, m *models.ChannelWatch) {
	info := m.GetProto()
	fmt.Fprintln(sb, "=============================")
	fmt.Fprintf(sb, "key: %s\n", m.Key())
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

	// skip schema print
	if !rs.printSchema {
		return
	}

	if info.Schema == nil {
		fmt.Fprintf(sb, "### Collection schema is empty!!! ###\n")
		return
	}
	fmt.Fprintf(sb, "Fields:\n")
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
		if field.DataType == schemapb.DataType_Array {
			fmt.Fprintf(sb, "\t - Element Type:  %s\n", field.ElementType.String())
		}
		// type params
		for _, kv := range field.TypeParams {
			fmt.Fprintf(sb, "\t - Type Param %s: %s\n", kv.Key, kv.Value)
		}
	}

	fmt.Fprintf(sb, "Enable Dynamic Schema: %t\n", info.Schema.EnableDynamicField)
}

func (rs *ChannelsWatched) printChannelWatchInfoJSON(sb *strings.Builder, model *models.ChannelWatch) {
	info := model.GetProto()
	m := make(map[string]any)
	m["key"] = model.Key()
	m["channel_name"] = info.Vchan.ChannelName

	pos := info.Vchan.SeekPosition
	if pos != nil {
		startTime, _ := utils.ParseTS(pos.Timestamp)
		m["position_id"] = pos.MsgID
		m["position_time"] = startTime
	}

	if rs.printSchema {
		m["schema"] = info.Schema
	}

	bs, err := json.Marshal(m)
	if err != nil {
		fmt.Println("failed to marshal watch info json:", err.Error())
		return
	}

	fmt.Fprintln(sb, string(bs))
}
