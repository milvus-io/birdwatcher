package show

import (
	"context"
	"os"

	"github.com/jedib0t/go-pretty/v6/table"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

type WalBroadcastParam struct {
	framework.ParamBase `use:"show wal-broadcast" desc:"display wal broadcast information from coordinator meta store"`
}

func (c *ComponentShow) WalBroadcastCommand(ctx context.Context, p *WalBroadcastParam) error {
	metas, err := common.ListWalBroadcast(ctx, c.client, c.metaPath)
	if err != nil {
		return err
	}
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.SetTitle("WAL Broadcast At Coordinator")
	t.AppendHeader(table.Row{"ID", "MessageType", "ResourceKey", "Acked", "State"})
	for _, meta := range metas {
		msg := message.NewBroadcastMutableMessageBeforeAppend(meta.Message.Payload, meta.Message.Properties)
		bh := msg.BroadcastHeader()
		rks := make([]string, 0, len(bh.ResourceKeys))
		for rk := range bh.ResourceKeys {
			rks = append(rks, rk.String())
		}
		// TODO: add message details
		t.AppendRow(table.Row{
			bh.BroadcastID,
			msg.MessageType().String(),
			rks,
			len(meta.AckedCheckpoints),
			meta.State.String(),
		})
	}
	t.Render()
	return nil
}
