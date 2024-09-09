package states

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/minio/minio-go/v7"
	"github.com/samber/lo"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/oss"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
	"github.com/milvus-io/birdwatcher/storage"
)

type ScanBinlogsParam struct {
	framework.ParamBase `use:"test scan-binlogs"`
	CollectionID        int64  `name:"collectionID"`
	MinioAddress        string `name:"minioAddr" default:"" desc:"the minio address to override, leave empty to use milvus.yaml value"`
	OutputFormat        string `name:"outputFmt" default:"stdout"`
}

func (s *InstanceState) TestScanBinlogsCommand(ctx context.Context, p *ScanBinlogsParam) error {
	params := []oss.MinioConnectParam{}
	if p.MinioAddress != "" {
		params = append(params, oss.WithMinioAddr(p.MinioAddress))
	}

	minioClient, bucketName, rootPath, err := s.GetMinioClientFromCfg(ctx, params...)
	if err != nil {
		return err
	}

	collection, err := common.GetCollectionByIDVersion(ctx, s.client, s.basePath, etcdversion.GetVersion(), p.CollectionID)
	if err != nil {
		return err
	}
	segments, err := common.ListSegmentsVersion(ctx, s.client, s.basePath, etcdversion.GetVersion(), func(segment *models.Segment) bool {
		return segment.CollectionID == collection.ID
	})
	if err != nil {
		return err
	}
	pkField, ok := collection.GetPKField()
	if !ok {
		return errors.New("collection does not has pk")
	}
	idField := lo.SliceToMap(collection.Schema.Fields, func(field models.FieldSchema) (int64, models.FieldSchema) {
		return field.FieldID, field
	})
	var f *os.File
	var pqWriter *storage.ParquetWriter
	selector := func(_ int64) bool { return true }
	switch p.OutputFormat {
	case "stdout":
		selector = func(_ int64) bool { return false }
	case "json":
		f, err = os.Create(fmt.Sprintf("%d.json", collection.ID))
		if err != nil {
			return err
		}
		defer f.Close()
	case "parquet":
		f, err = os.Create(fmt.Sprintf("%d.parquet", collection.ID))
		if err != nil {
			return err
		}
		pqWriter = storage.NewParquetWriter(collection)
		defer pqWriter.Flush(f)
	}
	for _, segment := range segments {
		deltalog, err := s.DownloadDeltalogs(ctx, minioClient, bucketName, rootPath, collection, segment)
		if err != nil {
			return err
		}

		s.ScanBinlogs(ctx, minioClient, bucketName, rootPath, collection, segment, selector, func(readers map[int64]*storage.BinlogReader) {
			iter, err := NewBinlogIterator(collection, readers)
			if err != nil {
				fmt.Println("failed to create iterator", err.Error())
				return
			}

			err = iter.Range(func(rowID, ts int64, pk storage.PrimaryKey, data map[int64]any) error {
				deleted := false
				deltalog.Range(func(delPk storage.PrimaryKey, delTs uint64) bool {
					if delPk.EQ(pk) && ts < int64(delTs) {
						deleted = true
						return false
					}
					return true
				})
				if deleted {
					return nil
				}
				output := lo.MapKeys(data, func(v any, k int64) string {
					return idField[k].Name
				})
				output[pkField.Name] = pk.GetValue()
				switch p.OutputFormat {
				case "stdout":
					fmt.Println(output)
				case "json":
					bs, err := json.Marshal(output)
					if err != nil {
						fmt.Println(err.Error())
						return err
					}
					f.Write(bs)
					f.Write([]byte("\n"))
				case "parquet":
					data[0] = rowID
					data[1] = ts
					data[pkField.FieldID] = pk.GetValue()
					writeParquetData(collection, pqWriter, rowID, ts, pk, output)
				}
				return nil
			})
			if err != nil {
				fmt.Println(err.Error())
			}
		})
	}

	return nil
}

// ScanBinlogs scans provided segment with delete record excluded.
func (s *InstanceState) ScanBinlogs(ctx context.Context, minioClient *minio.Client, bucketName string, rootPath string, collection *models.Collection, segment *models.Segment,
	selectField func(fieldID int64) bool, fn func(map[int64]*storage.BinlogReader),
) {
	pkField, has := lo.Find(collection.Schema.Fields, func(field models.FieldSchema) bool {
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
		field2Binlog := make(map[int64]*storage.BinlogReader)
		for _, fieldBinlog := range segment.GetBinlogs() {
			if fieldBinlog.FieldID != 0 && fieldBinlog.FieldID != 1 && fieldBinlog.FieldID != pkField.FieldID && !selectField(fieldBinlog.FieldID) {
				continue
			}
			binlog := fieldBinlog.Binlogs[idx]
			filePath := strings.Replace(binlog.LogPath, "ROOT_PATH", rootPath, -1)
			object, err := minioClient.GetObject(ctx, bucketName, filePath, minio.GetObjectOptions{})
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			reader, _, err := storage.NewBinlogReader(object)
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
	schema  *models.CollectionSchema
	readers map[int64]*storage.BinlogReader

	rowIDs     []int64
	timestamps []int64

	pks  storage.PrimaryKeys
	data map[int64][]any

	rows int
}

func NewBinlogIterator(collection *models.Collection, readers map[int64]*storage.BinlogReader) (*BinlogIterator, error) {
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

	var pks storage.PrimaryKeys
	pkField, _ := collection.GetPKField()
	pkReader := readers[pkField.FieldID]
	switch pkField.DataType {
	case models.DataTypeInt64:
		intPks, err := pkReader.NextInt64EventReader()
		if err != nil {
			return nil, err
		}
		iPks := storage.NewInt64PrimaryKeys(len(rowIDs))
		iPks.AppendRaw(intPks...)
		pks = iPks
	case models.DataTypeVarChar:
		strPks, err := pkReader.NextVarcharEventReader()
		if err != nil {
			return nil, err
		}
		sPks := storage.NewVarcharPrimaryKeys(len(rowIDs))
		sPks.AppendRaw(strPks...)
		pks = sPks
	}

	idField := lo.SliceToMap(collection.Schema.Fields, func(field models.FieldSchema) (int64, models.FieldSchema) {
		return field.FieldID, field
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
		schema:  &collection.Schema,
		readers: readers,

		rowIDs:     rowIDs,
		timestamps: timestamps,

		pks:  pks,
		data: data,

		rows: len(rowIDs),
	}, nil
}

func (iter *BinlogIterator) Range(fn func(rowID int64, ts int64, pk storage.PrimaryKey, data map[int64]any) error) error {
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

func readerToSlice(reader *storage.BinlogReader, field models.FieldSchema) ([]any, error) {
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

func writeParquetData(collection *models.Collection, pqWriter *storage.ParquetWriter, rowID, ts int64, pk storage.PrimaryKey, output map[string]any) error {
	pkField, _ := collection.GetPKField()
	for _, field := range collection.Schema.Fields {
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
			err = writeParquetField(pqWriter, field, output[field.Name])
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func writeParquetField(pqWriter *storage.ParquetWriter, field models.FieldSchema, v any) error {
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
