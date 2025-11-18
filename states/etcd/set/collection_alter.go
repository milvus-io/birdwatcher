package set

import (
	"context"
	"fmt"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

type FieldAttrParam struct {
	framework.ParamBase `use:"set field-attr" desc:"set field attribute for collection"`
	CollectionID        int64 `name:"collectionID" default:"0" desc:"collection id to update"`
	FieldID             int64 `name:"fieldID" default:"0" desc:"field id to update"`

	// target attributes
	IsClusteringKey bool `name:"clusterKey" default:"false" desc:"flags indicating whether to enable clusterKey"`
	Nullable        bool `name:"nullable" default:"false" desc:"flags indicating whether to enable nullable"`

	Run bool `name:"run" default:"false"`
}

func (c *ComponentSet) FieldAttrCommand(ctx context.Context, p *FieldAttrParam) error {
	if p.CollectionID <= 0 {
		return fmt.Errorf("invalid collection id(%d)", p.CollectionID)
	}
	if p.FieldID < 0 {
		return fmt.Errorf("invalid field id(%d)", p.FieldID)
	}

	alterField := func(field *schemapb.FieldSchema) {
		if field.FieldID != p.FieldID {
			return
		}
		field.IsClusteringKey = p.IsClusteringKey
		field.Nullable = p.Nullable
	}
	err := common.UpdateField(ctx, c.client, c.basePath, p.CollectionID, p.FieldID, alterField, !p.Run)
	if err != nil {
		return fmt.Errorf("failed to alter field (%s)", err.Error())
	}

	return nil
}
