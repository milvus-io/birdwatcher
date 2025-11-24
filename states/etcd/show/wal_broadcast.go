package show

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/jedib0t/go-pretty/v6/table"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

type WalBroadcastParam struct {
	framework.ParamBase `use:"show wal-broadcast" desc:"display wal broadcast information from coordinator meta store"`
	BroadcastID         int64 `name:"broadcast_id" default:"" desc:"broadcast id to show"`
	Detail              bool  `name:"detail" default:"false" desc:"display detail information of broadcast task"`
}

func (c *ComponentShow) WalBroadcastCommand(ctx context.Context, p *WalBroadcastParam) error {
	var metas []*streamingpb.BroadcastTask
	if p.BroadcastID != 0 {
		meta, err := common.ListWalBroadcastByID(ctx, c.client, c.metaPath, p.BroadcastID)
		if err != nil {
			return err
		}
		metas = append(metas, meta)
	} else {
		var err error
		if metas, err = common.ListWalBroadcast(ctx, c.client, c.metaPath); err != nil {
			return err
		}
	}
	if p.Detail {
		c.printDetail(metas)
		return nil
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
		acked, bitmap := c.getAcked(meta)
		t.AppendRow(table.Row{
			bh.BroadcastID,
			msg.MessageType().String(),
			rks,
			fmt.Sprintf("(%08b)%d/%d", bitmap, acked, len(bh.VChannels)),
			meta.State.String(),
		})
	}
	t.Render()
	return nil
}

func (c *ComponentShow) getAcked(meta *streamingpb.BroadcastTask) (int, []byte) {
	acked := 0
	bitmap := make([]byte, len(meta.AckedCheckpoints))
	for idx, checkpoint := range meta.AckedCheckpoints {
		if checkpoint != nil && checkpoint.TimeTick != 0 {
			acked++
			bitmap[idx] = 1
		}
	}
	return acked, bitmap
}

func (c *ComponentShow) printDetail(metas []*streamingpb.BroadcastTask) {
	for _, meta := range metas {
		msg := message.NewBroadcastMutableMessageBeforeAppend(meta.Message.Payload, meta.Message.Properties)
		bh := msg.BroadcastHeader()
		rks := make([]string, 0, len(bh.ResourceKeys))
		for rk := range bh.ResourceKeys {
			rks = append(rks, rk.String())
		}
		metaJSON, _ := protojson.Marshal(meta)
		acked, bitmap := c.getAcked(meta)
		fmt.Printf("=======Broadcast ID: %d====================\n", bh.BroadcastID)
		fmt.Printf("MessageType: %s\n", msg.MessageType().String())
		fmt.Printf("State: %s\n", meta.State.String())
		fmt.Printf("ResourceKeys: %s\n", strings.Join(rks, ", "))
		fmt.Printf("VChannels: %s\n", strings.Join(bh.VChannels, ", "))
		fmt.Printf("Acked: %s\n", fmt.Sprintf("(%08b)%d/%d", bitmap, acked, len(bh.VChannels)))
		fmt.Printf("Detail: \n%s\n\n", string(metaJSON))
	}
}
