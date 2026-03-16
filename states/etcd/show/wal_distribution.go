package show

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/samber/lo"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
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

	sessions, err := common.ListSessions(ctx, c.client, c.metaPath)
	if err != nil {
		return err
	}
	sessionMap := lo.SliceToMap(sessions, func(s *models.Session) (int64, *models.Session) { return s.ServerID, s })

	t := table.NewWriter()
	t.SetTitle("WAL Distribution At Coordinator")
	t.SetOutputMirror(os.Stdout)
	header := table.Row{"Channel", "StreamingNode", "State", "LastAssignTime"}
	if p.WithHistory {
		header = append(header, "History")
	}
	t.AppendHeader(header)
	for _, meta := range metas {
		channelInfo := types.NewPChannelInfoFromProto(meta.Channel)
		nodeInfo := types.NewStreamingNodeInfoFromProto(meta.Node)
		lastAssignTimestamp := time.Unix(int64(meta.LastAssignTimestampSeconds), 0)
		row := table.Row{
			channelInfo,
			formatStreamingNode(nodeInfo, sessionMap),
			strings.TrimPrefix(meta.State.String(), "PCHANNEL_META_STATE_"),
			lastAssignTimestamp,
		}
		if p.WithHistory {
			row = append(row, c.formatHistory(meta.Histories, sessionMap))
		}
		t.AppendRow(row)
	}
	t.Render()
	return nil
}

func (c *ComponentShow) formatHistory(histories []*streamingpb.PChannelAssignmentLog, sessionMap map[int64]*models.Session) string {
	if len(histories) == 0 {
		return ""
	}
	ss := make([]string, 0, len(histories))
	for _, history := range histories {
		nodeInfo := types.NewStreamingNodeInfoFromProto(history.Node)
		ss = append(ss, fmt.Sprintf("%s@%d->%s", types.AccessMode(history.AccessMode).String(), history.Term, formatStreamingNode(nodeInfo, sessionMap)))
	}
	return strings.Join(ss, "\n")
}

func formatStreamingNode(node types.StreamingNodeInfo, sessionMap map[int64]*models.Session) string {
	if sess, ok := sessionMap[node.ServerID]; ok {
		return fmt.Sprintf("%d(%s)", node.ServerID, sess.HostName)
	}
	return fmt.Sprintf("%d(NotFound)", node.ServerID)
}
