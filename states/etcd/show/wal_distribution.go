package show

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/olekukonko/tablewriter"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

const (
	walDistributionPrefix = "streamingcoord-meta/pchannel/"
)

type WALDistributionParam struct {
	framework.ParamBase `use:"show wal-distribution" desc:"display wal distribution information from coordinator meta store"`
	Channel             string `name:"channel" default:"" desc:"phsical channel name to filter"`
	WithHistory         bool   `name:"with-history" default:"false" desc:"display wal history that not assigned or on-removing"`
}

func (c *ComponentShow) WalDistributionCommand(ctx context.Context, p *WALDistributionParam) error {
	metas, err := common.ListWALDistribution(ctx, c.client, c.metaPath, p.Channel)
	if err != nil {
		return err
	}

	table := tablewriter.NewTable(os.Stdout)
	header := []string{"Channel", "StreamingNode", "State", "LastAssignTime"}
	if p.WithHistory {
		header = append(header, "History")
	}
	table.Header(header)
	for _, meta := range metas {
		channelInfo := types.NewPChannelInfoFromProto(meta.Channel)
		assignedTo := types.NewStreamingNodeInfoFromProto(meta.Node)
		lastAssignTimestamp := time.Unix(int64(meta.LastAssignTimestampSeconds), 0)
		row := []any{
			channelInfo,
			assignedTo,
			strings.TrimPrefix(meta.State.String(), "PCHANNEL_META_STATE_"),
			lastAssignTimestamp,
		}
		if p.WithHistory {
			row = append(row, c.formatHistory(meta.Histories))
		}
		table.Append(row)
	}
	table.Render()
	return nil
}

func (c *ComponentShow) formatHistory(histories []*streamingpb.PChannelAssignmentLog) string {
	if len(histories) == 0 {
		return ""
	}
	ss := make([]string, 0, len(histories))
	for _, history := range histories {
		assignedTo := types.NewStreamingNodeInfoFromProto(history.Node)
		ss = append(ss, fmt.Sprintf("%s@%d->%s", types.AccessMode(history.AccessMode).String(), history.Term, assignedTo.String()))
	}
	return strings.Join(ss, "\n")
}
