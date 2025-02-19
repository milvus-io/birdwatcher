package remove

import (
	"context"
	"fmt"

	"github.com/samber/lo"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
)

type DirtyImportingSegment struct {
	framework.ParamBase `use:"remove dirty-importing-segment" desc:"remove dirty importing segments with 0 rows"`
	CollectionID        int64 `name:"collection" default:"0" desc:"collection id to filter with"`
	Ts                  int64 `name:"ts" default:"0" desc:"only remove segments with ts less than this value"`
	Run                 bool  `name:"run" default:"false" desc:"flag to control actually run or dry"`
}

// DirtyImportingSegmentCommand returns command to remove
func (c *ComponentRemove) DirtyImportingSegmentCommand(ctx context.Context, p *DirtyImportingSegment) error {
	fmt.Println("start to remove dirty importing segment")
	segments, err := common.ListSegmentsVersion(ctx, c.client, c.basePath, etcdversion.GetVersion(), func(segment *models.Segment) bool {
		return (p.CollectionID == 0 || segment.CollectionID == p.CollectionID)
	})
	if err != nil {
		return err
	}

	groups := lo.GroupBy(segments, func(segment *models.Segment) int64 {
		return segment.CollectionID
	})

	for collectionID, segments := range groups {
		for _, segment := range segments {
			if segment.State == models.SegmentStateImporting {
				segmentTs := segment.GetDmlPosition().GetTimestamp()
				if segmentTs == 0 {
					segmentTs = segment.GetStartPosition().GetTimestamp()
				}
				if segment.NumOfRows == 0 && segmentTs < uint64(p.Ts) {
					fmt.Printf("collection %d, segment %d is dirty importing with 0 rows, remove it\n", collectionID, segment.ID)
					if p.Run {
						err := common.RemoveSegmentByID(ctx, c.client, c.basePath, segment.CollectionID, segment.PartitionID, segment.ID)
						if err != nil {
							fmt.Printf("failed to remove segment %d, err: %s\n", segment.ID, err.Error())
						}
					}
				} else {
					fmt.Printf("collection %d, segment %d is dirty importing with %d rows, ts=%d, skip it\n", collectionID, segment.ID, segment.NumOfRows, segmentTs)
				}
			}
		}
	}

	fmt.Println("finish to remove dirty importing segment")
	return nil
}
