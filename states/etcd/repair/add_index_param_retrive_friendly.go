package repair

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
)

type AddIndexParamParam struct {
	framework.ParamBase `use:"repair add_index_params" desc:"check index param and try to add param"`
	Collection          int64  `name:"collection" default:"0" desc:"collection id to filter with"`
	Key                 string `name:"key" default:"retrieve_friendly" desc:"add params key"`
	Value               string `name:"value" default:"true" desc:"add params value"`
	Run                 bool   `name:"run" default:"false" desc:"actual do repair"`
}

func (c *ComponentRepair) AddIndexParamsCommand(ctx context.Context, p *AddIndexParamParam) error {
	indexes, err := common.ListIndex(ctx, c.client, c.basePath, func(index *models.FieldIndex) bool {
		return p.Collection == 0 || p.Collection == index.GetProto().GetIndexInfo().GetCollectionID()
	})
	if err != nil {
		return err
	}
	newIndexes := make([]*models.FieldIndex, 0)
	for _, index := range indexes {
		indexType := ""
		for _, pair := range index.GetProto().GetIndexInfo().GetIndexParams() {
			if pair.Key == "index_type" {
				indexType = pair.Value
			}
		}
		if indexType != "DISKANN" && indexType != "HNSW" {
			continue
		}
		exist := false
		for _, pair := range index.GetProto().GetIndexInfo().GetIndexParams() {
			if pair.Key == p.Key {
				exist = true
				break
			}
		}
		if !exist {
			newIndex := proto.Clone(index.GetProto()).(*indexpb.FieldIndex)
			newIndex.IndexInfo.IndexParams = append(newIndex.IndexInfo.IndexParams, &commonpb.KeyValuePair{
				Key:   p.Key,
				Value: p.Value,
			})
			newIndexes = append(newIndexes, models.NewProtoWrapper[*indexpb.FieldIndex](newIndex, index.Key()))
		}
	}
	if !p.Run {
		fmt.Println("Dry run, after repair index:")
		for _, index := range newIndexes {
			printIndexV2(index.GetProto())
		}
		return nil
	}
	for _, index := range newIndexes {
		if err := writeRepairedIndex(c.client, c.basePath, index.GetProto()); err != nil {
			return err
		}
	}
	afterRepairIndexes, err := common.ListIndex(ctx, c.client, c.basePath, func(index *models.FieldIndex) bool {
		return p.Collection == 0 || p.Collection == index.GetProto().GetIndexInfo().GetCollectionID()
	})
	if err != nil {
		return err
	}
	for _, index := range afterRepairIndexes {
		printIndexV2(index.GetProto())
	}
	return nil
}
