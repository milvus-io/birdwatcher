package states

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/expr-lang/expr"
	"github.com/minio/minio-go/v7"
	"github.com/samber/lo"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/proto/v2.0/schemapb"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
	"github.com/milvus-io/birdwatcher/storage"
)

type ScanBinlogParams struct {
	framework.ParamBase `use:"scan-binlog" desc:"test expr"`
	CollectionID        int64    `name:"collection" default:"0"`
	SegmentID           int64    `name:"segment" default:"0"`
	Fields              []string `name:"fields"`
	Expr                string   `name:"expr"`
	MinioAddress        string   `name:"minioAddr"`
	Action              string   `name:"action" default:"count"`
}

func (s *InstanceState) ScanBinlogCommand(ctx context.Context, p *ScanBinlogParams) error {
	collection, err := common.GetCollectionByIDVersion(ctx, s.client, s.basePath, etcdversion.GetVersion(), p.CollectionID)
	if err != nil {
		return err
	}
	fmt.Println("=== Checking collection schema ===")
	pkField, ok := collection.GetPKField()
	if !ok {
		return errors.New("pk field not found")
	}
	fmt.Printf("PK Field [%d] %s\n", pkField.FieldID, pkField.Name)

	fieldsMap := make(map[string]struct{})
	for _, field := range p.Fields {
		fieldsMap[field] = struct{}{}
	}

	fields := make(map[int64]models.FieldSchema) // make([]models.FieldSchema, 0, len(p.Fields))

	for _, fieldSchema := range collection.Schema.Fields {
		if _, ok := fieldsMap[fieldSchema.Name]; ok {
			fmt.Printf("Output Field %s field id %d\n", fieldSchema.Name, fieldSchema.FieldID)
			fields[fieldSchema.FieldID] = fieldSchema
		}
	}

	segments, err := common.ListSegmentsVersion(ctx, s.client, s.basePath, etcdversion.GetVersion(), func(s *models.Segment) bool {
		return (p.SegmentID == 0 || p.SegmentID == s.ID) && p.CollectionID == s.CollectionID
	})
	if err != nil {
		return err
	}

	minioClient, bucketName, rootPath, err := s.GetMinioClientFromCfg(ctx, p.MinioAddress)
	if err != nil {
		fmt.Println("Failed to create folder,", err.Error())
	}

	fmt.Printf("=== start to execute \"%s\" task with filter expresion: \"%s\" ===\n", p.Action, p.Expr)

	// prepare action dataset
	// count
	var count int64
	// locate do nothing
	// dedup
	ids := make(map[any]struct{})
	dedupResult := make(map[any]int64)

	for _, segment := range segments {
		var pkBinlog *models.FieldBinlog
		targetFieldBinlogs := []*models.FieldBinlog{}
		for _, fieldBinlog := range segment.GetBinlogs() {
			_, inTarget := fields[fieldBinlog.FieldID]
			if inTarget {
				targetFieldBinlogs = append(targetFieldBinlogs, fieldBinlog)
			}
			if fieldBinlog.FieldID == pkField.FieldID {
				pkBinlog = fieldBinlog
			}
		}

		getObject := func(binlogPath string) (*minio.Object, error) {
			logPath := strings.Replace(binlogPath, "ROOT_PATH", rootPath, -1)
			return minioClient.GetObject(ctx, bucketName, logPath, minio.GetObjectOptions{})
		}
		if pkBinlog == nil && segment.State != models.SegmentStateGrowing {
			fmt.Printf("PK Binlog not found, segment %d\n", segment.ID)
			continue
		}

		for idx, binlog := range pkBinlog.Binlogs {
			pkObject, err := getObject(binlog.LogPath)
			if err != nil {
				return err
			}
			fieldObjects := make(map[int64]storage.ReadSeeker)
			for _, fieldBinlog := range targetFieldBinlogs {
				binlog := fieldBinlog.Binlogs[idx]
				targetObject, err := getObject(binlog.LogPath)
				if err != nil {
					return err
				}
				fieldObjects[fieldBinlog.FieldID] = targetObject
			}

			err = s.scanBinlogs(pkObject, fieldObjects, func(pk storage.PrimaryKey, offset int, values map[int64]any) error {
				if len(p.Expr) != 0 {
					env := lo.MapKeys(values, func(_ any, fid int64) string {
						return fields[fid].Name
					})
					program, err := expr.Compile(p.Expr, expr.Env(env))
					if err != nil {
						return err
					}

					output, err := expr.Run(program, env)
					if err != nil {
						fmt.Println("failed to run expression, err: ", err.Error())
					}

					match, ok := output.(bool)
					if !ok {
						return errors.Newf("filter expression result not bool but %T", output)
					}

					if !match {
						return nil
					}
				}

				switch strings.ToLower(p.Action) {
				case "count":
					count++
				case "locate":
					fmt.Printf("entry found, segment %d offset %d, pk: %v\n", segment.ID, offset, pk.GetValue())
					fmt.Printf("binlog batch %d, pk binlog %s\n", idx, binlog.LogPath)
				case "dedup":
					_, ok := ids[pk.GetValue()]
					if ok {
						dedupResult[pk.GetValue()]++
					}
					ids[pk.GetValue()] = struct{}{}
				}
				return nil
			})
			if err != nil {
				return err
			}
		}
	}

	switch strings.ToLower(p.Action) {
	case "count":
		fmt.Printf("Total %d entries found", count)
	case "dedup":
		total := len(dedupResult)
		fmt.Printf("%d duplicated entries found\n", total)
		var i int64
		for pk, cnt := range dedupResult {
			if i > 10 {
				break
			}
			fmt.Printf("PK[%s] %v duplicated %d times\n", pkField.Name, pk, cnt+1)
			i++
		}
	default:
	}

	return nil
}

func (s *InstanceState) scanBinlogs(pk storage.ReadSeeker, fields map[int64]storage.ReadSeeker, scanner func(pk storage.PrimaryKey, offset int, values map[int64]any) error) error {
	pkReader, desc, err := storage.NewBinlogReader(pk)
	if err != nil {
		return err
	}

	fieldDesc := make(map[int64]storage.DescriptorEvent)
	fieldData := make(map[int64]any)

	var readerErr error

	lo.MapValues(fields, func(r storage.ReadSeeker, k int64) *storage.BinlogReader {
		reader, desc, err := storage.NewBinlogReader(r)
		if err != nil {
			readerErr = err
			return nil
		}
		fieldDesc[k] = desc

		var data any
		switch desc.PayloadDataType {
		case schemapb.DataType_Int64:
			data, err = reader.NextInt64EventReader()
		case schemapb.DataType_VarChar:
			data, err = reader.NextVarcharEventReader()
		case schemapb.DataType_Float:
			data, err = reader.NextFloat32EventReader()
		case schemapb.DataType_Double:
			data, err = reader.NextFloat64EventReader()
		}
		if err != nil {
			readerErr = err
			return nil
		}
		fieldData[k] = data
		return reader
	})

	if readerErr != nil {
		return readerErr
	}

	var pks []storage.PrimaryKey

	switch desc.PayloadDataType {
	case schemapb.DataType_Int64:
		values, err := pkReader.NextInt64EventReader()
		if err != nil {
			return err
		}
		pks = lo.Map(values, func(id int64, _ int) storage.PrimaryKey { return storage.NewInt64PrimaryKey(id) })
	case schemapb.DataType_VarChar:
		values, err := pkReader.NextVarcharEventReader()
		if err != nil {
			return err
		}
		pks = lo.Map(values, func(id string, _ int) storage.PrimaryKey { return storage.NewVarCharPrimaryKey(id) })
	}

	for idx, pk := range pks {
		fields := make(map[int64]any)
		for fid, data := range fieldData {
			switch fieldDesc[fid].PayloadDataType {
			case schemapb.DataType_Int64:
				values := data.([]int64)
				fields[fid] = values[idx]
			case schemapb.DataType_VarChar:
				values := data.([]string)
				fields[fid] = values[idx]
			case schemapb.DataType_Float:
				values := data.([]float32)
				fields[fid] = values[idx]
			case schemapb.DataType_Double:
				values := data.([]float64)
				fields[fid] = values[idx]
			}
		}
		err = scanner(pk, idx, fields)
		if err != nil {
			fmt.Println("scan err", err.Error())
			return err
		}
	}

	return nil
}
