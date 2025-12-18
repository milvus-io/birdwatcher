package healthz

import (
	"context"
	"fmt"

	"github.com/milvus-io/birdwatcher/states/etcd/common"
	metakv "github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type MixedBinlogs struct {
	checkItemBase
}

func newMixedBinlogs() *MixedBinlogs {
	return &MixedBinlogs{
		checkItemBase: checkItemBase{
			name:        "MIXED_BINLOGS",
			description: `Check whethe segment have both v1 & v2 binlog records`,
		},
	}
}

func (i *MixedBinlogs) Check(ctx context.Context, client metakv.MetaKV, basePath string) ([]*HealthzCheckReport, error) {
	segments, err := common.ListSegments(ctx, client, basePath)
	if err != nil {
		return nil, err
	}
	// validIDs := lo.SliceToMap(segments, func(segment *models.Segment) (int64, struct{}) { return segment.ID, struct{}{} })

	var results []*HealthzCheckReport

	for _, segment := range segments {
		v1 := typeutil.NewSet[int64]()
		v2 := typeutil.NewSet[int64]()
		for _, binlog := range segment.GetBinlogs() {
			if len(binlog.ChildFields) > 0 {
				v2.Insert(binlog.ChildFields...)
			} else {
				v1.Insert(binlog.FieldID)
			}
		}
		result := v1.Intersection(v2)
		if result.Len() > 0 {
			results = append(results, &HealthzCheckReport{
				Item: i.Name(),
				Msg:  fmt.Sprintf("Segment %d has both v1 & v2 binlog records", segment.GetID()),
				Extra: map[string]any{
					"segment_id":        segment.GetID(),
					"duplicated_fields": result.Collect(),
				},
			})
		}
	}

	return results, nil
}
