package repair

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
)

type RepairSegmentPartDropParam struct {
	framework.ParamBase `use:"repair segment-part-dropping" desc:"mark segments of partitions in dropping state to dropped"`
	Collection          int64 `name:"collection" default:"0" desc:"collection id to filter with"`
	Partition           int64 `name:"partition" default:"0" desc:"partition id to filter with"`
	Run                 bool  `name:"run" default:"false" desc:"actual do repair"`
}

func (c *ComponentRepair) RepairSegmentPartDropCommand(ctx context.Context, p *RepairSegmentPartDropParam) error {
	if p.Collection == 0 {
		return errors.New("collection id not provided")
	}

	partitions, err := common.ListCollectionPartitions(ctx, c.client, c.basePath, p.Collection)
	if err != nil {
		return err
	}

	// get all partition ids in dropping & dropped state
	targetPartitions := make(map[int64]struct{})
	lo.ForEach(partitions, func(partition *models.Partition, _ int) {
		if p.Partition != 0 && partition.GetProto().GetPartitionID() != p.Partition {
			return
		}
		state := partition.GetProto().GetState()
		if state == etcdpb.PartitionState_PartitionDropping || state == etcdpb.PartitionState_PartitionDropped {
			targetPartitions[partition.GetProto().GetPartitionID()] = struct{}{}
		}
	})

	segments, err := common.ListSegments(ctx, c.client, c.basePath, func(info *models.Segment) bool {
		if info.GetState() == commonpb.SegmentState_Dropped {
			return false
		}
		_, ok := targetPartitions[info.GetPartitionID()]
		return ok
	})
	if err != nil {
		return err
	}

	groups := lo.GroupBy(segments, func(segment *models.Segment) int64 {
		return segment.GetPartitionID()
	})

	var total int64
	for partitionID, segments := range groups {
		fmt.Printf("=== partition %d has %d segments ===\n", partitionID, len(segments))
		for _, segment := range segments {
			fmt.Printf("Segment %d, State %s\n", segment.GetID(), segment.GetState().String())
			total += segment.GetNumOfRows()
			if p.Run {
				err := common.RemoveSegment(ctx, c.client, c.basePath, segment.SegmentInfo)
				if err != nil {
					fmt.Println(err.Error())
				}
			}
		}
	}
	fmt.Printf("total %d segment, %d row num matched\n", len(segments), total)

	return nil
}
