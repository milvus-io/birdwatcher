package healthz

import (
	"context"
	"fmt"
	"sort"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	metakv "github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

type StorageV1BinlogSchemaMismatch struct {
	checkItemBase
}

func newStorageV1BinlogSchemaMismatch() *StorageV1BinlogSchemaMismatch {
	return &StorageV1BinlogSchemaMismatch{
		checkItemBase: checkItemBase{
			name:        "STORAGE_V1_BINLOG_SCHEMA_MISMATCH",
			description: `Checks whether storage v1 segments (storage version 0 or 1) have insert binlogs for every field in the collection schema.`,
		},
	}
}

func (i *StorageV1BinlogSchemaMismatch) Check(ctx context.Context, client metakv.MetaKV, basePath string) ([]*HealthzCheckReport, error) {
	collections, err := common.ListCollections(ctx, client, basePath)
	if err != nil {
		return nil, err
	}

	collectionByID := make(map[int64]*models.Collection, len(collections))
	for _, collection := range collections {
		collectionByID[collection.GetProto().GetID()] = collection
	}

	segments, err := common.ListSegments(ctx, client, basePath)
	if err != nil {
		return nil, err
	}

	var results []*HealthzCheckReport
	for _, segment := range segments {
		if segment.GetLevel() == datapb.SegmentLevel_L0 {
			continue
		}
		if segment.GetStorageVersion() != 0 && segment.GetStorageVersion() != 1 {
			continue
		}

		collection, ok := collectionByID[segment.GetCollectionID()]
		if !ok {
			continue
		}

		schemaFieldIDs := make(map[int64]struct{})
		for _, field := range collection.GetProto().GetSchema().GetFields() {
			schemaFieldIDs[field.GetFieldID()] = struct{}{}
		}

		binlogFieldIDs := make(map[int64]struct{})
		for _, fieldBinlog := range segment.GetBinlogs() {
			binlogFieldIDs[fieldBinlog.FieldID] = struct{}{}
		}

		missingFieldIDs := missingIDs(schemaFieldIDs, binlogFieldIDs)
		if len(missingFieldIDs) == 0 {
			continue
		}

		results = append(results, &HealthzCheckReport{
			Item: i.Name(),
			Msg:  fmt.Sprintf("Segment %d storage v1 binlogs are missing collection schema fields", segment.GetID()),
			Extra: map[string]any{
				"segment_id":        segment.GetID(),
				"collection_id":     segment.GetCollectionID(),
				"storage_version":   segment.GetStorageVersion(),
				"missing_field_ids": missingFieldIDs,
				"schema_field_ids":  sortedIDs(schemaFieldIDs),
				"binlog_field_ids":  sortedIDs(binlogFieldIDs),
			},
		})
	}

	return results, nil
}

func missingIDs(expected, actual map[int64]struct{}) []int64 {
	missing := make([]int64, 0)
	for id := range expected {
		if _, ok := actual[id]; !ok {
			missing = append(missing, id)
		}
	}
	sort.Slice(missing, func(i, j int) bool { return missing[i] < missing[j] })
	return missing
}

func sortedIDs(ids map[int64]struct{}) []int64 {
	result := make([]int64, 0, len(ids))
	for id := range ids {
		result = append(result, id)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}
