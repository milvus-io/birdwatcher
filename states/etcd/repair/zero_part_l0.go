package repair

import (
	"context"
	"fmt"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

type ZeroParitiionL0Param struct {
	framework.ParamBase `use:"repair zero-partition-l0" desc:"Set partitionID to non-zero value for L0 segments with partitionID 0"`

	Collection        int64 `name:"collection" default:"0" desc:"collection id to filter with"`
	Segment           int64 `name:"segment" default:"0" desc:"segment id to filter with"`
	TargetPartitionID int64 `name:"targetPartitionID" default:"-1" desc:"the partition id to set for the segment"`
	Run               bool  `name:"run" default:"false" desc:"actual do repair"`
}

func (c *ComponentRepair) ZeroPartitionL0SegmentCommand(ctx context.Context, p *ZeroParitiionL0Param) error {
	segments, err := common.ListSegments(ctx, c.client, c.basePath, func(info *models.Segment) bool {
		return info.GetLevel() == datapb.SegmentLevel_L0 &&
			(p.Collection == 0 || info.GetCollectionID() == p.Collection) &&
			(p.Segment == 0 || info.GetID() == p.Segment) &&
			info.GetPartitionID() == 0
	})
	if err != nil {
		fmt.Println("failed to list segments", err.Error())
		return nil
	}

	fmt.Printf("%d segment found\n", len(segments))
	for _, info := range segments {
		fmt.Printf("suspect segment %d found:\n", info.GetID())
		fmt.Printf("SegmentID: %d CollectionID: %d PartitionID: %d, State: %s, Row Count:%d\n",
			info.ID, info.CollectionID, info.PartitionID, info.State.String(), info.NumOfRows)
	}

	if !p.Run {
		return nil
	}

	for _, info := range segments {
		info.PartitionID = p.TargetPartitionID
		err := common.UpdateSegment(ctx, c.client, info)
		if err != nil {
			fmt.Printf("update segment %d partitionID failed: %s\n", info.GetID(), err.Error())
			continue
		}
		fmt.Printf("update segment %d partitionID to %d succeed\n", info.GetID(), p.TargetPartitionID)
	}

	return nil
}
