package repair

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type MixedBinlogsParam struct {
	framework.ParamBase `use:"repair mixed-binlogs" desc:"repair collection info"`
	Run                 bool `name:"run" default:"false" desc:"run the mixed binlogs repair command"`
}

func (c *ComponentRepair) MixedBinlogsCommand(ctx context.Context, p *MixedBinlogsParam) error {
	segments, err := common.ListSegments(ctx, c.client, c.basePath)
	if err != nil {
		return err
	}

	type target struct {
		segment          *models.Segment
		duplicatedFields []int64
		targetBinlogs    []*models.FieldBinlog
		verdict          string
	}

	collections := make(map[int64]*models.Collection)
	getCollection := func(collID int64) (*models.Collection, error) {
		coll, ok := collections[collID]
		if ok {
			return coll, nil
		}
		var err error
		coll, err = common.GetCollectionByIDVersion(ctx, c.client, c.basePath, collID)
		return coll, err
	}

	var results []*target
	for _, segment := range segments {
		v1 := typeutil.NewSet[int64]()
		v2 := typeutil.NewSet[int64]()
		v1binlogs := make(map[int64]*models.FieldBinlog)
		v2binlogs := make(map[int64]*models.FieldBinlog)

		for _, binlog := range segment.GetBinlogs() {
			if len(binlog.ChildFields) > 0 {
				v2.Insert(binlog.ChildFields...)
				v2binlogs[binlog.FieldID] = binlog
			} else {
				v1.Insert(binlog.FieldID)
				v1binlogs[binlog.FieldID] = binlog
			}
		}

		result := v1.Intersection(v2)
		if result.Len() > 0 {
			coll, err := getCollection(segment.CollectionID)
			if err != nil {
				return err
			}
			fieldNumber := len(coll.GetProto().Schema.Fields)
			var targetBinlogs []*models.FieldBinlog
			var verdict string
			switch {
			case fieldNumber == v1.Len():
				targetBinlogs = lo.Values(v2binlogs)
				verdict = "v1 haves all fields, remove v2 binlogs"
			case fieldNumber == v2.Len():
				targetBinlogs = lo.Values(v1binlogs)
				verdict = "v2 haves all fields, remove v1 binlogs"
			default:
				return errors.New("neither v1 or v2 binlog has all fields")
			}
			results = append(results, &target{
				segment:          segment,
				duplicatedFields: result.Collect(),
				targetBinlogs:    targetBinlogs,
				verdict:          verdict,
			})
		}
	}

	if !p.Run {
		fmt.Println("Dry Run")
		for _, result := range results {
			fmt.Printf("Segment %d has both v1 & v2 binlog records\n", result.segment.GetID())
			fmt.Printf("Duplicated fields: %v\n", result.duplicatedFields)
			fmt.Println(result.verdict)
			segment := result.segment
			for _, binlog := range result.targetBinlogs {
				fmt.Println("plan to remove key: ", fmt.Sprintf("%s/datacoord-meta/binlog/%d/%d/%d/%d", c.basePath, segment.CollectionID, segment.PartitionID, segment.ID, binlog.FieldID))
			}
		}
		return nil
	}

	for _, result := range results {
		fmt.Printf("Segment %d has both v1 & v2 binlog records\n", result.segment.GetID())
		fmt.Printf("Duplicated fields: %v\n", result.duplicatedFields)
		fmt.Println(result.verdict)
		segment := result.segment
		for _, binlog := range result.targetBinlogs {
			key := fmt.Sprintf("%s/datacoord-meta/binlog/%d/%d/%d/%d", c.basePath, segment.CollectionID, segment.PartitionID, segment.ID, binlog.FieldID)
			fmt.Println("plan to remove key: ", key)
			err := c.client.Remove(ctx, key)
			if err != nil {
				return err
			}
			fmt.Printf("Remove key %s done\n", key)
		}
	}
	return nil
}
