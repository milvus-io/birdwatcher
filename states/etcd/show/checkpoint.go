package show

import (
	"context"
	"fmt"
	"path"
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/proto/v2.0/internalpb"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
	"github.com/milvus-io/birdwatcher/utils"
)

type CheckpointParam struct {
	framework.ParamBase `use:"show checkpoint" desc:"list checkpoint collection vchannels" alias:"checkpoints,cp"`
	CollectionID        int64 `name:"collection" default:"0" desc:"collection id to filter with"`
}

// CheckpointCommand returns show checkpoint command.
func (c *ComponentShow) CheckpointCommand(ctx context.Context, p *CheckpointParam) (*Checkpoints, error) {
	coll, err := common.GetCollectionByIDVersion(context.Background(), c.client, c.basePath, etcdversion.GetVersion(), p.CollectionID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get collection")
	}

	checkpoints := make([]*Checkpoint, 0, len(coll.Channels))
	for _, channel := range coll.Channels {
		checkpoint := &Checkpoint{
			Channel: &models.Channel{
				PhysicalName: channel.PhysicalName,
				VirtualName:  channel.VirtualName,
			},
		}
		var cp *models.MsgPosition
		var segmentID int64
		var err error

		cp, err = c.getChannelCheckpoint(ctx, channel.VirtualName)
		if err == nil {
			checkpoint.Source = "Channel Checkpoint"
			checkpoint.Checkpoint = cp
			checkpoints = append(checkpoints, checkpoint)
			continue
		}

		cp, segmentID, err = c.getCheckpointFromSegments(ctx, p.CollectionID, channel.VirtualName)
		if err == nil {
			checkpoint.Source = fmt.Sprintf("from segment id %d", segmentID)
			checkpoint.Checkpoint = cp
			checkpoints = append(checkpoints, checkpoint)
			continue
		}

		checkpoints = append(checkpoints, checkpoint)
	}

	return framework.NewListResult[Checkpoints](checkpoints), nil
}

type Checkpoint struct {
	Channel    *models.Channel
	Source     string
	Checkpoint *models.MsgPosition
}

type Checkpoints struct {
	framework.ListResultSet[*Checkpoint]
}

func (rs *Checkpoints) PrintAs(format framework.Format) string {
	switch format {
	case framework.FormatDefault, framework.FormatPlain:
		sb := &strings.Builder{}
		for _, checkpoint := range rs.Data {
			if checkpoint.Checkpoint == nil {
				fmt.Fprintf(sb, "Vchannel %s checkpoint not found, fallback to collection start pos\n", checkpoint.Channel.VirtualName)
				continue
			}
			t, _ := utils.ParseTS(checkpoint.Checkpoint.GetTimestamp())
			fmt.Fprintf(sb, "vchannel %s seek to %v, cp channel: %s, Source: %s\n",
				checkpoint.Channel.VirtualName, t, checkpoint.Checkpoint.ChannelName,
				checkpoint.Source)
		}
		return sb.String()
	default:
	}
	return ""
}

func (c *ComponentShow) getChannelCheckpoint(ctx context.Context, channelName string) (*models.MsgPosition, error) {
	prefix := path.Join(c.basePath, "datacoord-meta", "channel-cp", channelName)
	results, _, err := common.ListProtoObjects[internalpb.MsgPosition](ctx, c.client, prefix)
	if err != nil {
		return nil, err
	}

	if len(results) != 1 {
		return nil, fmt.Errorf("expected 1 position but got %d", len(results))
	}

	pos := models.NewMsgPosition(&results[0])
	return pos, nil
}

func (c *ComponentShow) getCheckpointFromSegments(ctx context.Context, collID int64, vchannel string) (*models.MsgPosition, int64, error) {
	segments, err := common.ListSegmentsVersion(ctx, c.client, c.basePath, etcdversion.GetVersion(), func(info *models.Segment) bool {
		return info.CollectionID == collID && info.InsertChannel == vchannel
	})
	if err != nil {
		fmt.Printf("fail to list segment for channel %s, err: %s\n", vchannel, err.Error())
		return nil, 0, err
	}
	fmt.Printf("find segments to list checkpoint for %s, segment found %d\n", vchannel, len(segments))
	var segmentID int64
	var pos *models.MsgPosition
	for _, segment := range segments {
		if segment.State != models.SegmentStateFlushed &&
			segment.State != models.SegmentStateGrowing &&
			segment.State != models.SegmentStateFlushing {
			continue
		}
		// skip all empty segment
		if segment.GetDmlPosition() == nil && segment.GetStartPosition() == nil {
			continue
		}
		var segPos *models.MsgPosition

		if segment.GetDmlPosition() != nil {
			segPos = segment.GetDmlPosition()
		} else {
			segPos = segment.GetStartPosition()
		}

		if pos == nil || segPos.GetTimestamp() < pos.GetTimestamp() {
			pos = segPos
			segmentID = segment.ID
		}
	}

	return pos, segmentID, nil
}
