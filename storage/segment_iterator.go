package storage

import (
	"context"
	"io"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/storage/common"
	"github.com/milvus-io/birdwatcher/storage/tasks"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

type SegmentIterator struct {
	segment *models.Segment
	schema  *schemapb.CollectionSchema

	filters   []EntryFilter
	id2Schema map[int64]*schemapb.FieldSchema

	translator func(binlog string) (common.ReadSeeker, error)
	scanTask   tasks.ScanTask
}

func (si *SegmentIterator) Range(ctx context.Context) error {
	fields := lo.Keys(si.id2Schema)
	reader, err := NewSegmentReader(si.segment, fields, si.translator)
	if err != nil {
		return err
	}

	var pk common.PrimaryKey
	pkSchema := lo.FindOrElse(si.schema.Fields, nil, func(field *schemapb.FieldSchema) bool {
		return field.IsPrimaryKey
	})
	switch pkSchema.DataType {
	case schemapb.DataType_Int64:
		pk = &common.Int64PrimaryKey{}
	case schemapb.DataType_VarChar:
		pk = &common.VarCharPrimaryKey{}
	default:
		return errors.Newf("unssupported primary key type %s", pkSchema.DataType.String())
	}

	for {
		recordBatch, batchInfo, err := reader.Next(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}

		for i := range recordBatch.Len() {
			values := make(map[int64]any)
			for _, fieldID := range fields {
				fieldSchema := si.id2Schema[fieldID]
				arr := recordBatch.Column(fieldID)
				val, ok := DeserializeItem(arr, si.id2Schema[fieldID].DataType, i)
				if !ok {
					return errors.Newf("failed to deserialize field %d, %d-th item", fieldID, i)
				}

				values[fieldID] = val
				if fieldSchema.IsPrimaryKey {
					pk.SetValue(val)
				}
			}

			if err := si.scan(ctx, pk, batchInfo, i, values); err != nil {
				return err
			}
		}
	}
}

func (si *SegmentIterator) scan(_ context.Context, pk common.PrimaryKey, batchInfo *common.BatchInfo, offset int, values map[int64]any) error {
	for _, filter := range si.filters {
		if !filter.Match(pk, values[1].(int64), values) {
			return nil
		}
	}
	return si.scanTask.Scan(pk, batchInfo, offset, values)
}

func NewSegmentIterator(segment *models.Segment,
	schema *schemapb.CollectionSchema,

	filters []EntryFilter,
	id2Schema map[int64]*schemapb.FieldSchema,

	translator func(binlog string) (common.ReadSeeker, error),
	scanTask tasks.ScanTask,
) *SegmentIterator {
	return &SegmentIterator{
		segment:    segment,
		schema:     schema,
		filters:    filters,
		id2Schema:  id2Schema,
		translator: translator,
		scanTask:   scanTask,
	}
}
