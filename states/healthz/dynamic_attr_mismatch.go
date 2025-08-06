package healthz

import (
	"context"

	"github.com/milvus-io/birdwatcher/states/etcd/common"
	metakv "github.com/milvus-io/birdwatcher/states/kv"
)

type dynamicAttrMismatch struct {
	checkItemBase
}

func newDynamicAttrMismatch() dynamicAttrMismatch {
	return dynamicAttrMismatch{
		checkItemBase: checkItemBase{
			name:        "DYNAMIC_ATTR_MISMATCH",
			description: `Checks whether dynamic schema attributes are same in collection & field schema.`,
		},
	}
}

func (c dynamicAttrMismatch) Check(ctx context.Context, client metakv.MetaKV, basePath string) ([]*HealthzCheckReport, error) {
	collections, err := common.ListCollections(ctx, client, basePath)
	if err != nil {
		return nil, err
	}

	var results []*HealthzCheckReport

	for _, collection := range collections {
		collFlag := collection.GetProto().GetSchema().GetEnableDynamicField()
		for _, field := range collection.GetProto().GetSchema().GetFields() {
			if field.GetIsDynamic() && !collFlag {
				results = append(results, &HealthzCheckReport{
					Item: c.Name(),
					Msg:  "Dynamic attribute mismatch",
					Extra: map[string]any{
						"collection":       collection.GetProto().GetID(),
						"dynamic_field":    field.GetName(),
						"dynamic_field_id": field.GetFieldID(),
					},
				})
				break
			}
		}
	}
	return results, nil
}
