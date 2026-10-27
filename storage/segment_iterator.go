package storage

import (
	"context"
	"fmt"
	"io"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/storage/common"
	"github.com/milvus-io/birdwatcher/storage/tasks"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
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
	return si.RangeReader(ctx, reader)
}

// RangeReader applies the same row filters and task to every storage layout.
// It takes ownership of reader, including on errors and early task completion.
func (si *SegmentIterator) RangeReader(ctx context.Context, reader SegmentRecordReader) (err error) {
	var rowsRead int64
	end := common.ScanOperation(ctx, "iterator.rows segment=%d", si.segment.GetID())
	defer func() { end(err, fmt.Sprintf("rows_read=%d task_counter=%d", rowsRead, si.scanTask.Counter())) }()
	defer reader.Close()
	fields := lo.Keys(si.id2Schema)
	var pk common.PrimaryKey
	pkSchema := lo.FindOrElse(si.schema.Fields, nil, func(field *schemapb.FieldSchema) bool {
		return field.IsPrimaryKey
	})
	if pkSchema == nil {
		return errors.New("pk field not found")
	}
	switch pkSchema.DataType {
	case schemapb.DataType_Int64:
		pk = &common.Int64PrimaryKey{}
	case schemapb.DataType_VarChar:
		pk = &common.VarCharPrimaryKey{}
	default:
		return errors.Newf("unssupported primary key type %s", pkSchema.DataType.String())
	}

	// offset is the index of item in current batch
	offset := 0
	// batchIdx is the index of file batch
	batchIdx := -1
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		readEnd := common.ScanOperation(ctx, "reader.next rows_read=%d", rowsRead)
		recordBatch, batchInfo, err := reader.Next(ctx)
		if err != nil {
			readEnd(err)
		} else {
			readEnd(nil, fmt.Sprintf("batch=%d segment_offset=%d rows=%d", batchInfo.BatchIdx, batchInfo.SegmentOffset, recordBatch.Len()))
		}
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		// reset offset if batch index changed
		if batchInfo.BatchIdx != batchIdx {
			batchIdx = batchInfo.BatchIdx
			offset = 0
		}

		for i := range recordBatch.Len() {
			values := make(map[int64]any)
			for _, fieldID := range fields {
				fieldSchema := si.id2Schema[fieldID]
				arr := recordBatch.Column(fieldID)
				if arr == nil {
					return errors.Newf("missing field %d in segment %d", fieldID, si.segment.GetID())
				}
				val, ok := DeserializeItem(arr, si.id2Schema[fieldID].DataType, i)
				if !ok {
					return errors.Newf("failed to deserialize field %d, %d-th item", fieldID, i)
				}

				values[fieldID] = val
				if fieldSchema.IsPrimaryKey {
					if err := pk.SetValue(val); err != nil {
						return err
					}
				}
			}

			if err := si.scan(ctx, pk, batchInfo, offset, values); err != nil {
				return err
			}
			offset++
			rowsRead++
		}
		common.ScanDebug(ctx, "rows.processed batch=%d rows_read=%d task_counter=%d next_offset=%d", batchInfo.BatchIdx, rowsRead, si.scanTask.Counter(), offset)
	}
}

func (si *SegmentIterator) scan(_ context.Context, pk common.PrimaryKey, batchInfo *common.BatchInfo, offset int, values map[int64]any) error {
	ts, ok := values[1].(int64)
	if !ok {
		return errors.Newf("missing or invalid timestamp in segment %d", batchInfo.SegmentID)
	}
	for _, filter := range si.filters {
		match, err := filter.Match(pk, ts, values)
		if err != nil {
			return err
		}
		if !match {
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
