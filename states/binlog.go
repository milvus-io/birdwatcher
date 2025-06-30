package states

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/minio/minio-go/v7"
	"github.com/samber/lo"

	"github.com/milvus-io/birdwatcher/models"
	binlogv1 "github.com/milvus-io/birdwatcher/storage/binlog/v1"
	storagecommon "github.com/milvus-io/birdwatcher/storage/common"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

// ScanBinlogs scans provided segment with delete record excluded.
func (s *InstanceState) ScanBinlogs(ctx context.Context, minioClient *minio.Client, bucketName string, rootPath string, collection *models.Collection, segment *models.Segment,
	selectField func(fieldID int64) bool, fn func(map[int64]*binlogv1.BinlogReader),
) {
	pkField, has := lo.Find(collection.GetProto().Schema.Fields, func(field *schemapb.FieldSchema) bool {
		return field.IsPrimaryKey
	})
	if !has {
		return
	}

	pkFieldData, has := lo.Find(segment.GetBinlogs(), func(fieldBinlog *models.FieldBinlog) bool {
		return fieldBinlog.FieldID == pkField.FieldID
	})
	if !has {
		return
	}

	for idx := range pkFieldData.Binlogs {
		field2Binlog := make(map[int64]*binlogv1.BinlogReader)
		for _, fieldBinlog := range segment.GetBinlogs() {
			if fieldBinlog.FieldID != 0 && fieldBinlog.FieldID != 1 && fieldBinlog.FieldID != pkField.FieldID && !selectField(fieldBinlog.FieldID) {
				continue
			}
			binlog := fieldBinlog.Binlogs[idx]
			filePath := strings.ReplaceAll(binlog.LogPath, "ROOT_PATH", rootPath)
			object, err := minioClient.GetObject(ctx, bucketName, filePath, minio.GetObjectOptions{})
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			reader, err := binlogv1.NewBinlogReader(object)
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			field2Binlog[fieldBinlog.FieldID] = reader
		}

		fn(field2Binlog)
	}
}

type BinlogIterator struct {
	schema  *schemapb.CollectionSchema
	readers map[int64]*binlogv1.BinlogReader

	rowIDs     []int64
	timestamps []int64

	pks  storagecommon.PrimaryKeys
	data map[int64][]any

	rows int
}

func NewBinlogIterator(collection *models.Collection, readers map[int64]*binlogv1.BinlogReader) (*BinlogIterator, error) {
	rowIDReader := readers[0]
	rowIDs, err := rowIDReader.NextInt64EventReader()
	if err != nil {
		return nil, err
	}

	tsReader := readers[1]
	timestamps, err := tsReader.NextInt64EventReader()
	if err != nil {
		return nil, err
	}

	var pks storagecommon.PrimaryKeys
	pkField, _ := collection.GetPKField()
	pkReader := readers[pkField.FieldID]
	switch pkField.DataType {
	case models.DataTypeInt64:
		intPks, err := pkReader.NextInt64EventReader()
		if err != nil {
			return nil, err
		}
		iPks := storagecommon.NewInt64PrimaryKeys(len(rowIDs))
		iPks.AppendRaw(intPks...)
		pks = iPks
	case models.DataTypeVarChar:
		strPks, err := pkReader.NextVarcharEventReader()
		if err != nil {
			return nil, err
		}
		sPks := storagecommon.NewVarcharPrimaryKeys(len(rowIDs))
		sPks.AppendRaw(strPks...)
		pks = sPks
	}

	idField := lo.SliceToMap(collection.GetProto().Schema.Fields, func(field *schemapb.FieldSchema) (int64, models.FieldSchema) {
		return field.FieldID, models.NewFieldSchemaFromBase(field)
	})
	data := make(map[int64][]any)
	for fieldID, reader := range readers {
		if fieldID < 100 || fieldID == pkField.FieldID {
			continue
		}
		field := idField[fieldID]
		column, err := readerToSlice(reader, field)
		if err != nil {
			return nil, err
		}
		data[fieldID] = column
	}

	return &BinlogIterator{
		schema:  collection.GetProto().Schema,
		readers: readers,

		rowIDs:     rowIDs,
		timestamps: timestamps,

		pks:  pks,
		data: data,

		rows: len(rowIDs),
	}, nil
}

func (iter *BinlogIterator) Range(fn func(rowID int64, ts int64, pk storagecommon.PrimaryKey, data map[int64]any) error) error {
	for i := 0; i < iter.rows; i++ {
		row := make(map[int64]any)
		for field, columns := range iter.data {
			row[field] = columns[i]
		}
		err := fn(iter.rowIDs[i], iter.timestamps[i], iter.pks.Get(i), row)
		if err != nil {
			fmt.Println(err.Error())
			return err
		}
	}
	return nil
}

func toAny[T any](v T, _ int) any { return v }

func readerToSlice(reader *binlogv1.BinlogReader, field models.FieldSchema) ([]any, error) {
	switch field.DataType {
	case models.DataTypeBool:
		val, err := reader.NextBoolEventReader()
		return lo.Map(val, toAny[bool]), err
	case models.DataTypeInt8:
		val, err := reader.NextInt8EventReader()
		return lo.Map(val, toAny[int8]), err
	case models.DataTypeInt16:
		val, err := reader.NextInt16EventReader()
		return lo.Map(val, toAny[int16]), err
	case models.DataTypeInt32:
		val, err := reader.NextInt32EventReader()
		return lo.Map(val, toAny[int32]), err
	case models.DataTypeInt64:
		val, err := reader.NextInt64EventReader()
		return lo.Map(val, toAny[int64]), err
	case models.DataTypeFloat:
		val, err := reader.NextFloat32EventReader()
		return lo.Map(val, toAny[float32]), err
	case models.DataTypeDouble:
		val, err := reader.NextFloat64EventReader()
		return lo.Map(val, toAny[float64]), err
	case models.DataTypeVarChar, models.DataTypeString:
		val, err := reader.NextVarcharEventReader()
		return lo.Map(val, toAny[string]), err
	case models.DataTypeJSON:
		val, err := reader.NextByteSliceEventReader()
		return lo.Map(val, toAny[[]byte]), err
	case models.DataTypeFloatVector:
		val, err := reader.NextFloatVectorEventReader()
		return lo.Map(val, toAny[[]float32]), err
	case models.DataTypeBinaryVector:
		val, err := reader.NextBinaryVectorEventReader()
		return lo.Map(val, toAny[[]byte]), err
	default:
		return nil, errors.Newf("data type %s not supported yet", field.DataType.String())
	}
}

func writeParquetData(collection *models.Collection, pqWriter *binlogv1.ParquetWriter, rowID, ts int64, pk storagecommon.PrimaryKey, output map[string]any) error {
	pkField, _ := collection.GetPKField()
	for _, field := range collection.GetProto().Schema.Fields {
		var err error
		switch field.FieldID {
		case 0: // RowID
			err = pqWriter.AppendInt64("RowID", rowID)
		case 1: // Timestamp
			err = pqWriter.AppendInt64("Timestamp", ts)
		case pkField.FieldID:
			switch pkField.DataType {
			case models.DataTypeInt64:
				err = pqWriter.AppendInt64(pkField.Name, pk.GetValue().(int64))
			case models.DataTypeVarChar:
				err = pqWriter.AppendString(pkField.Name, pk.GetValue().(string))
			}
		default:
			err = writeParquetField(pqWriter, models.NewFieldSchemaFromBase(field), output[field.Name])
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func writeParquetField(pqWriter *binlogv1.ParquetWriter, field models.FieldSchema, v any) error {
	var err error
	switch field.DataType {
	case models.DataTypeBool:
		pqWriter.AppendBool(field.Name, v.(bool))
	case models.DataTypeInt8:
		pqWriter.AppendInt8(field.Name, v.(int8))
	case models.DataTypeInt16:
		pqWriter.AppendInt16(field.Name, v.(int16))
	case models.DataTypeInt32:
		pqWriter.AppendInt32(field.Name, v.(int32))
	case models.DataTypeInt64:
		pqWriter.AppendInt64(field.Name, v.(int64))
	case models.DataTypeFloat:
		pqWriter.AppendFloat32(field.Name, v.(float32))
	case models.DataTypeDouble:
		pqWriter.AppendFloat64(field.Name, v.(float64))
	case models.DataTypeVarChar, models.DataTypeString:
		pqWriter.AppendString(field.Name, v.(string))
	case models.DataTypeJSON:
		pqWriter.AppendBytes(field.Name, v.([]byte))
	case models.DataTypeFloatVector:
		pqWriter.AppendFloatVector(field.Name, v.([]float32))
	case models.DataTypeBinaryVector:
		pqWriter.AppendBinaryVector(field.Name, v.([]byte))
	default:
		return errors.Newf("data type %v not supported", field.DataType)
	}
	return err
}
