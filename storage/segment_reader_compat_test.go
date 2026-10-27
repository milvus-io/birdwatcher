package storage

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/birdwatcher/models"
	binlogv1 "github.com/milvus-io/birdwatcher/storage/binlog/v1"
	"github.com/milvus-io/birdwatcher/storage/common"
	"github.com/milvus-io/birdwatcher/storage/manifest"
	"github.com/milvus-io/birdwatcher/storage/tasks"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func TestScanResultsMatchAcrossStorageVersions(t *testing.T) {
	fields := scanTestFields(schemapb.DataType_Int64)
	rows := [][]any{{int64(1), int64(10), int64(1)}, {int64(1), int64(30), int64(2)}, {int64(2), int64(10), int64(3)}}
	var arrowFields []arrow.Field
	for _, fid := range []int64{100, 1, 101} {
		arrowFields = append(arrowFields, arrow.Field{
			Name: fmt.Sprint(fid), Type: arrow.PrimitiveTypes.Int64,
			Metadata: arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{fmt.Sprint(fid)}),
		})
	}
	for _, version := range []int64{1, 2, 3} {
		for _, deleted := range []bool{false, true} {
			t.Run(fmt.Sprintf("v%d/deletes=%t", version, deleted), func(t *testing.T) {
				for _, action := range []string{"count", "dedup", "expr", "count-distinct"} {
					t.Run(action, func(t *testing.T) {
						dataRows := rows
						if action == "count-distinct" {
							dataRows = [][]any{{int64(1), int64(10), int64(7)}, {int64(1), int64(30), int64(7)}, {int64(2), int64(10), int64(8)}}
						}
						var debug bytes.Buffer
						ctx, stop := common.WithScanDebug(context.Background(), &debug)
						defer stop()
						var logs []*datapb.FieldBinlog
						objects := make(map[string][]byte)
						if version == 1 {
							for col, fid := range []int64{100, 1, 101} {
								var values [][]any
								for _, row := range dataRows {
									values = append(values, []any{row[col]})
								}
								name := fmt.Sprint(fid)
								pq := scanTestParquet(t, []arrow.Field{arrowFields[col]}, values, 2)
								objects[name] = scanTestLegacyBinlog(t, schemapb.DataType_Int64, binlogv1.InsertEventType, fid, pq)
								logs = append(logs, &datapb.FieldBinlog{FieldID: fid, Binlogs: []*datapb.Binlog{{LogPath: name, EntriesNum: 3}}})
							}
						} else {
							objects["packed"] = scanTestParquet(t, arrowFields, dataRows, 2)
							logs = []*datapb.FieldBinlog{{FieldID: 0, Binlogs: []*datapb.Binlog{{LogPath: "packed", EntriesNum: 3}}}}
						}
						segment := models.NewSegment(&datapb.SegmentInfo{ID: 20, StorageVersion: version, ManifestPath: scanTestSegment().GetManifestPath()}, "", func() ([]*datapb.FieldBinlog, []*datapb.FieldBinlog, []*datapb.FieldBinlog, []*datapb.FieldBinlog, error) {
							return logs, nil, nil, nil, nil
						})
						var sources []*scanTestSource
						open := func(name string) (common.ReadSeeker, error) {
							source := &scanTestSource{Reader: bytes.NewReader(objects[name])}
							sources = append(sources, source)
							return common.WithScanReadLogging(ctx, name, source), nil
						}
						var filters []EntryFilter
						if deleted {
							filters = append(filters, NewDeltalogFilter(map[any]uint64{int64(1): 20}))
						}
						var task tasks.ScanTask = tasks.NewCountTask()
						want := int64(3)
						if deleted {
							want = 2
						}
						if action == "dedup" {
							task = tasks.NewDedupTask(10, models.FieldSchema{FieldID: 100})
							want = 1
							if deleted {
								want = 0
							}
						}
						if action == "expr" {
							filter, err := NewExprFilter(fields, "value >= 2")
							require.NoError(t, err)
							filters = append(filters, filter)
							want = 2
						}
						if action == "count-distinct" {
							var err error
							task, err = tasks.NewCountDistinctTask(fields[101])
							require.NoError(t, err)
							want = 2
						}
						iter := NewSegmentIterator(segment, &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{fields[100]}}, filters, fields, open, task)
						if version == 3 {
							m := &manifest.Manifest{ColumnGroups: []manifest.ColumnGroup{{Columns: []string{"100", "1", "101"}, Format: "parquet", Files: []manifest.ColumnGroupFile{{Path: "packed", EndIndex: 3}}}}}
							reader, err := NewManifestSegmentReader(segment, m, fields, open)
							require.NoError(t, err)
							require.NoError(t, iter.RangeReader(ctx, reader))
						} else {
							require.NoError(t, iter.Range(ctx))
						}
						stop()
						if version == 1 {
							require.Contains(t, debug.String(), "io.Read ")
						} else {
							require.Contains(t, debug.String(), "io.ReadAt")
						}
						if version < 3 {
							require.Contains(t, debug.String(), "legacy.arrow.next")
						} else {
							require.Contains(t, debug.String(), "parquet.arrow.next")
						}
						require.Equal(t, want, task.Counter())
						for _, source := range sources {
							require.Equal(t, 1, source.closed)
						}
					})
				}
			})
		}
	}
}
