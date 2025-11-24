package repair

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

var (
	repairBroadcastTaskModeReset  string = "reset"
	repairBroadcastTaskModeRemove string = "remove"
)

type WALBroadcastTaskParam struct {
	framework.ParamBase `use:"repair wal-broadcast-task" desc:"repair wal broadcast task"`
	Mode                string `name:"mode" default:"reset" desc:"reset or remove wal broadcast task"`
	BroadcastID         int64  `name:"broadcast_id" default:"" desc:"broadcast id to repair"`
	Run                 bool   `name:"run" default:"false" desc:"actual do repair"`
}

func (c *ComponentRepair) WALBroadcastTaskCommand(ctx context.Context, p *WALBroadcastTaskParam) error {
	if p.Mode != repairBroadcastTaskModeReset && p.Mode != repairBroadcastTaskModeRemove {
		return fmt.Errorf("invalid mode: %s", p.Mode)
	}

	meta, err := common.ListWalBroadcastByID(ctx, c.client, c.basePath, p.BroadcastID)
	if err != nil {
		if errors.Is(err, common.ErrBroadcastTaskNotFound) {
			fmt.Printf("broadcast task not found with broadcast ID %d\n", p.BroadcastID)
			return nil
		}
		return errors.Wrap(err, "failed to list wal broadcast task")
	}

	detail, err := protojson.Marshal(meta)
	if err != nil {
		return errors.Wrap(err, "failed to marshal broadcast task")
	}
	fmt.Printf("Broadcast Task Detail: \n%s\n", string(detail))

	if !p.Run {
		return nil
	}

	switch p.Mode {
	case repairBroadcastTaskModeReset:
		msg := message.NewBroadcastMutableMessageBeforeAppend(meta.Message.Payload, meta.Message.Properties)
		meta.AckedCheckpoints = make([]*streamingpb.AckedCheckpoint, len(msg.BroadcastHeader().VChannels))
		meta.AckedVchannelBitmap = make([]byte, len(msg.BroadcastHeader().VChannels))
		meta.State = streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING
		if err := common.SaveWalBroadcastTask(ctx, c.client, c.basePath, p.BroadcastID, meta); err != nil {
			return errors.Wrap(err, "failed to save wal broadcast task")
		}
		fmt.Printf("wal broadcast task reseted with broadcast ID %d\n", p.BroadcastID)
	case repairBroadcastTaskModeRemove:
		if err := common.RemoveWalBroadcastTask(ctx, c.client, c.basePath, p.BroadcastID); err != nil {
			return errors.Wrap(err, "failed to remove wal broadcast task")
		}
		fmt.Printf("wal broadcast task removed with broadcast ID %d\n", p.BroadcastID)
	default:
		return fmt.Errorf("invalid mode: %s", p.Mode)
	}
	return nil
}
