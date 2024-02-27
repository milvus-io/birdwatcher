package states

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/gosuri/uilive"
	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/proto/v2.0/schemapb"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
	"github.com/milvus-io/birdwatcher/storage"
	"github.com/minio/minio-go/v7"
	"github.com/samber/lo"
)

type CheckPartitionKeyParam struct {
	framework.ParamBase `use:"check-partiton-key" desc:"check partition key field file"`
	Storage             string `name:"storage" default:"auto" desc:"storage service configuration mode"`
	StopIfErr           bool   `name:"stopIfErr" default:"true"`
	OutputPrimaryKey    bool   `name:"outputPK" default:"true" desc:"print error record primary key info in stdout mode"`
	MinioAddress        string `name:"minioAddr" default:"" desc:"the minio address to override, leave empty to use milvus.yaml value"`
	OutputFormat        string `name:"outputFmt" default:"stdout"`

	CollectionID int64 `name:"collection" default:"0" desc:"target collection to scan, default scan all partition key collections"`
}

var errQuickExit = errors.New("quick exit")

func (s *InstanceState) CheckPartitionKeyCommand(ctx context.Context, p *CheckPartitionKeyParam) error {
	collections, err := common.ListCollectionsVersion(ctx, s.client, s.basePath, etcdversion.GetVersion(), func(collection *models.Collection) bool {
		return p.CollectionID == 0 || collection.ID == p.CollectionID
	})
	if err != nil {
		return err
	}

	var minioClient *minio.Client
	var bucketName, rootPath string

	minioClient, bucketName, rootPath, err = s.GetMinioClientFromCfg(ctx, p.MinioAddress)
	if err != nil {
		return err
	}

	type suspectCollection struct {
		collection   *models.Collection
		partitions   []*models.Partition
		partIndex    map[int64]uint32 // partition id to hash index
		idName       map[int64]string
		partKeyField models.FieldSchema
		pkField      models.FieldSchema
		tsField      models.FieldSchema
	}

	var suspectCollections []*suspectCollection

	for _, collection := range collections {
		partKeyField, enablePartKey := lo.Find(collection.Schema.Fields, func(field models.FieldSchema) bool {
			return field.IsPartitionKey
		})
		if !enablePartKey {
			continue
		}
		pkField, _ := lo.Find(collection.Schema.Fields, func(field models.FieldSchema) bool {
			return field.IsPrimaryKey
		})
		tsField, _ := lo.Find(collection.Schema.Fields, func(field models.FieldSchema) bool {
			return field.FieldID == 1
		})

		partitions, err := common.ListCollectionPartitions(ctx, s.client, s.basePath, collection.ID)
		if err != nil {
			continue
		}

		partIdx := lo.SliceToMap(partitions, func(partition *models.Partition) (int64, uint32) {
			splits := strings.Split(partition.Name, "_")
			if len(splits) < 2 {
				return -1, 0
			}
			index, err := strconv.ParseInt(splits[len(splits)-1], 10, 64)
			if err != nil {
				return -1, 0
			}
			if (index >= int64(len(partitions))) || (index < 0) {
				return -1, 0
			}
			return partition.ID, uint32(index)
		})
		idName := lo.SliceToMap(partitions, func(partition *models.Partition) (int64, string) {
			return partition.ID, partition.Name
		})

		suspectCollections = append(suspectCollections, &suspectCollection{
			collection:   collection,
			partitions:   partitions,
			partIndex:    partIdx,
			idName:       idName,
			partKeyField: partKeyField,
			pkField:      pkField,
			tsField:      tsField,
		})
	}

	for _, susCol := range suspectCollections {
		collection := susCol.collection
		partitions := susCol.partitions
		partKeyField := susCol.partKeyField
		partIdx := susCol.partIndex
		pkField, _ := collection.GetPKField()
		idField := lo.SliceToMap(collection.Schema.Fields, func(field models.FieldSchema) (int64, models.FieldSchema) {
			return field.FieldID, field
		})

		fmt.Printf("Start to check collection %s id = %d\n", collection.Schema.Name, collection.ID)

		segments, err := common.ListSegmentsVersion(ctx, s.client, s.basePath, etcdversion.GetVersion(), func(segment *models.Segment) bool {
			return segment.CollectionID == collection.ID
		})

		if err != nil {
			return err
		}

		var collectionErrs int
		var found bool

		fmt.Printf("Partition number: %d, Segment number %d\n", len(partitions), len(segments))
		progressDisplay := uilive.New()
		progressFmt := "Scan segment ... %d%%(%d/%d) %s\n"
		progressDisplay.Start()
		fmt.Fprintf(progressDisplay, progressFmt, 0, 0, len(segments), "")

		for idx, segment := range segments {
			if segment.State == models.SegmentStateDropped || segment.State == models.SegmentStateSegmentStateNone {
				continue
			}
			var errCnt int
			err := func() error {
				var f *os.File
				var pqWriter *storage.ParquetWriter
				selector := func(_ int64) bool { return true }
				switch p.OutputFormat {
				case "stdout":
					selector = func(field int64) bool { return field == partKeyField.FieldID }
				case "json":
					f, err = os.Create(fmt.Sprintf("%d-%d.json", collection.ID, segment.ID))
					if err != nil {
						return err
					}
				case "parquet":
					f, err = os.Create(fmt.Sprintf("%d-%d.parquet", collection.ID, segment.ID))
					if err != nil {
						return err
					}
					pqWriter = storage.NewParquetWriter(collection)
				}
				deltalog, err := s.DownloadDeltalogs(ctx, minioClient, bucketName, rootPath, collection, segment)
				if err != nil {
					return err
				}

				s.ScanBinlogs(ctx, minioClient, bucketName, rootPath, collection, segment, selector, func(readers map[int64]*storage.BinlogReader) {
					targetIndex := partIdx[segment.PartitionID]
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

						partKeyValue := data[partKeyField.FieldID]
						var hashVal uint32
						switch partKeyField.DataType {
						case models.DataTypeInt64:
							hashVal, _ = Hash32Int64(partKeyValue.(int64))
						case models.DataTypeVarChar, models.DataTypeString:
							hashVal = HashString2Uint32(partKeyValue.(string))
						default:
							return errors.Newf("unexpected partition key field type %v", partKeyField.DataType)
						}
						if (hashVal % uint32(len(partitions))) == targetIndex {
							return nil
						}
						errCnt++
						found = true

						output := lo.MapKeys(data, func(v any, k int64) string {
							return idField[k].Name
						})
						output[pkField.Name] = pk.GetValue()
						switch p.OutputFormat {
						case "stdout":
							if p.OutputPrimaryKey {
								fmt.Printf("PK %v partition does not follow partition key rule (%s=%v)\n", pk.GetValue(), partKeyField.Name, partKeyValue)
							}
							if p.StopIfErr {
								return errQuickExit
							}
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
					if err != nil && !errors.Is(err, errQuickExit) {
						fmt.Println(err.Error())
					}
				})
				return nil
			}()
			if err != nil && !errors.Is(err, errQuickExit) {
				return err
			}
			if p.StopIfErr && found {
				break
			}
			progress := idx * 100 / len(segments)
			status := fmt.Sprintf("%d [%s]", segment.ID, colorReady.Sprint("done"))
			if errCnt > 0 {
				// fmt.Printf("Segment %d of collection %s find %d partition-key error\n", segment.ID, collection.Schema.Name, errCnt)
				collectionErrs += errCnt
				status = fmt.Sprintf("%d [%s](%d)", segment.ID, colorPending.Sprint("error"), errCnt)
			}

			fmt.Fprintf(progressDisplay, progressFmt, progress, idx+1, len(segments), status)
		}
		progressDisplay.Stop()
		fmt.Println()
		if p.StopIfErr {
			if found {
				fmt.Printf("Collection %s found partition key error\n", collection.Schema.Name)
			} else {
				fmt.Printf("Collection %s all data OK!\n", collection.Schema.Name)
			}
		} else {
			fmt.Printf("Collection %s found %d partition key error\n", collection.Schema.Name, collectionErrs)
		}

	}
	return nil
}

// type TestDownloadDeltalogParam struct {
// 	framework.ParamBase `use:"test download-deltalogs"`
// 	RootPath            string `name:"rootPath" default:"files"`
// }

// func (s *InstanceState) TestDownloadDeltalogCommand(ctx context.Context, p *TestDownloadDeltalogParam) error {
// 	// client, bucketName, _, err := s.GetMinioClientFromCfg(ctx) //getMinioAccess()
// 	// if err != nil {
// 	// 	return err
// 	// }
// 	// segments, err := common.ListSegmentsVersion(ctx, s.client, s.basePath, etcdversion.GetVersion())
// 	// if err != nil {
// 	// 	return err
// 	// }
// 	// for _, segment := range segments {
// 	// 	// s.DownloadDeltalogs(ctx, client, bucketName, p.RootPath, collection, segment)
// 	// }
// 	return nil
// }

func (s *InstanceState) DownloadDeltalogs(ctx context.Context, client *minio.Client, bucket, rootPath string, collection *models.Collection, segment *models.Segment) (*storage.DeltaData, error) {
	pkField, has := lo.Find(collection.Schema.Fields, func(field models.FieldSchema) bool {
		return field.IsPrimaryKey
	})
	if !has {
		return nil, errors.New("pk not found")
	}
	data := storage.NewDeltaData(schemapb.DataType(pkField.DataType), 0)
	for _, delFieldBinlog := range segment.GetDeltalogs() {
		for _, binlog := range delFieldBinlog.Binlogs {
			filePath := strings.Replace(binlog.LogPath, "ROOT_PATH", rootPath, -1)
			result, err := client.GetObject(ctx, bucket, filePath, minio.GetObjectOptions{})
			if err != nil {
				fmt.Println(err.Error())
				continue
			}

			reader, err := storage.NewDeltalogReader(result)
			if err != nil {
				fmt.Println(err.Error())
				continue
			}

			var deltaData *storage.DeltaData
			for err == nil {
				deltaData, err = reader.NextEventReader(schemapb.DataType(pkField.DataType))
				if err == nil {
					err = data.Merge(deltaData)
					if err != nil {
						return nil, err
					}
				}
			}
			if err != io.EOF {
				return nil, err
			}
		}
	}
	return data, nil
}
