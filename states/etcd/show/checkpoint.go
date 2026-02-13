package show

import (
	"context"
	"fmt"
	"path"
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/utils"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
)

type CheckpointParam struct {
	framework.DataSetParam `use:"show checkpoint" desc:"list checkpoint collection vchannels" alias:"checkpoints,cp"`
	CollectionID           int64 `name:"collection" default:"0" desc:"collection id to filter with"`
}

// CheckpointCommand returns show checkpoint command.
func (c *ComponentShow) CheckpointCommand(ctx context.Context, p *CheckpointParam) (*framework.PresetResultSet, error) {
	coll, err := common.GetCollectionByIDVersion(ctx, c.client, c.metaPath, p.CollectionID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get collection")
	}

	channels := coll.Channels()
	checkpoints := make([]*Checkpoint, 0, len(channels))
	for _, channel := range channels {
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

	return framework.NewPresetResultSet(framework.NewListResult[Checkpoints](checkpoints), framework.NameFormat(p.Format)), nil
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
			t, _ := utils.ParseTS(checkpoint.Checkpoint.GetProto().GetTimestamp())
			fmt.Fprintf(sb, "vchannel %s seek to %v, cp channel: %s, Source: %s\n",
				checkpoint.Channel.VirtualName, t, checkpoint.Checkpoint.GetProto().ChannelName,
				checkpoint.Source)
		}
		return sb.String()
	case framework.FormatJSON:
		return rs.printAsJSON()
	default:
	}
	return ""
}

func (rs *Checkpoints) printAsJSON() string {
	type CheckpointJSON struct {
		VirtualChannel  string `json:"virtual_channel"`
		PhysicalChannel string `json:"physical_channel"`
		Source          string `json:"source"`
		Timestamp       string `json:"timestamp,omitempty"`
		ChannelName     string `json:"channel_name,omitempty"`
		HasCheckpoint   bool   `json:"has_checkpoint"`
	}

	type OutputJSON struct {
		Checkpoints []CheckpointJSON `json:"checkpoints"`
		Total       int              `json:"total"`
	}

	output := OutputJSON{
		Checkpoints: make([]CheckpointJSON, 0, len(rs.Data)),
		Total:       len(rs.Data),
	}

	for _, cp := range rs.Data {
		cpJSON := CheckpointJSON{
			VirtualChannel:  cp.Channel.VirtualName,
			PhysicalChannel: cp.Channel.PhysicalName,
			Source:          cp.Source,
			HasCheckpoint:   cp.Checkpoint != nil,
		}
		if cp.Checkpoint != nil {
			t, _ := utils.ParseTS(cp.Checkpoint.GetProto().GetTimestamp())
			cpJSON.Timestamp = t.Format("2006-01-02 15:04:05")
			cpJSON.ChannelName = cp.Checkpoint.GetProto().ChannelName
		}
		output.Checkpoints = append(output.Checkpoints, cpJSON)
	}

	return framework.MarshalJSON(output)
}

func (c *ComponentShow) getChannelCheckpoint(ctx context.Context, channelName string) (*models.MsgPosition, error) {
	prefix := path.Join(c.metaPath, "datacoord-meta", "channel-cp", channelName)
	results, keys, err := common.ListProtoObjects[msgpb.MsgPosition](ctx, c.client, prefix)
	if err != nil {
		return nil, err
	}

	if len(results) != 1 {
		return nil, fmt.Errorf("expected 1 position but got %d", len(results))
	}

	pos := models.NewProtoWrapper(results[0], keys[0])
	return pos, nil
}

func (c *ComponentShow) getCheckpointFromSegments(ctx context.Context, collID int64, vchannel string) (*models.MsgPosition, int64, error) {
	segments, err := common.ListSegments(ctx, c.client, c.metaPath, func(info *models.Segment) bool {
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
		if segment.State != commonpb.SegmentState_Flushed &&
			segment.State != commonpb.SegmentState_Growing &&
			segment.State != commonpb.SegmentState_Flushing {
			continue
		}
		// skip all empty segment
		if segment.GetDmlPosition() == nil && segment.GetStartPosition() == nil {
			continue
		}
		var segPos *models.MsgPosition

		if segment.GetDmlPosition() != nil {
			segPos = models.NewProtoWrapper(segment.GetDmlPosition(), "")
		} else {
			segPos = models.NewProtoWrapper(segment.GetStartPosition(), "")
		}

		if pos == nil || segPos.GetProto().GetTimestamp() < pos.GetProto().GetTimestamp() {
			pos = segPos
			segmentID = segment.ID
		}
	}

	return pos, segmentID, nil
}
