package remove

import (
	"context"
	"errors"
	"fmt"

	"github.com/samber/lo"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
)

type SegmentOrphan struct {
	framework.ParamBase `use:"remove segment-orphan" desc:"remove orphan segments that collection meta already gone"`
	CollectionID        int64 `name:"collection" default:"0" desc:"collection id to filter with"`
	Run                 bool  `name:"run" default:"false" desc:"flag to control actually run or dry"`
}

// SegmentOrphanCommand returns command to remove
func (c *ComponentRemove) SegmentOrphanCommand(ctx context.Context, p *SegmentOrphan) error {
	segments, err := common.ListSegments(ctx, c.client, c.basePath, func(segment *models.Segment) bool {
		return (p.CollectionID == 0 || segment.CollectionID == p.CollectionID)
	})
	if err != nil {
		return err
	}

	groups := lo.GroupBy(segments, func(segment *models.Segment) int64 {
		return segment.CollectionID
	})

	for collectionID, segments := range groups {
		_, err := common.GetCollectionByIDVersion(ctx, c.client, c.basePath, collectionID)
		if errors.Is(err, common.ErrCollectionNotFound) {
			// print segments
			fmt.Printf("Collection %d missing, orphan segments: %v\n", collectionID, lo.Map(segments, func(segment *models.Segment, idx int) int64 {
				return segment.ID
			}))

			if p.Run {
				for _, segment := range segments {
					err := common.RemoveSegmentByID(ctx, c.client, c.basePath, segment.CollectionID, segment.PartitionID, segment.ID)
					if err != nil {
						fmt.Printf("failed to remove segment %d, err: %s\n", segment.ID, err.Error())
					}
				}
			}
		}
	}
	return nil
}
