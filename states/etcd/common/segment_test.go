package common

import (
	"context"
	"path"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

type segmentListKV struct {
	kv.MetaKV
	data           map[string]string
	loadedPrefixes []string
	loadedKeys     []string
}

func (s *segmentListKV) Load(ctx context.Context, key string, opts ...kv.LoadOption) (string, error) {
	s.loadedKeys = append(s.loadedKeys, key)
	value, ok := s.data[key]
	if !ok {
		return "", kv.ErrKeyNotFound
	}
	return value, nil
}

func (s *segmentListKV) LoadWithPrefix(ctx context.Context, prefix string, opts ...kv.LoadOption) ([]string, []string, error) {
	s.loadedPrefixes = append(s.loadedPrefixes, prefix)

	keys := make([]string, 0)
	for key := range s.data {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)

	values := make([]string, 0, len(keys))
	for _, key := range keys {
		values = append(values, s.data[key])
	}
	return keys, values, nil
}

func TestListSegmentsByUsesCollectionPrefix(t *testing.T) {
	ctx := context.Background()
	basePath := "root"
	cli := &segmentListKV{data: map[string]string{
		path.Join(basePath, DCPrefix, SegmentMetaPrefix, "100", "10", "1"): mustSegmentValue(t, &datapb.SegmentInfo{ID: 1, CollectionID: 100, PartitionID: 10}),
		path.Join(basePath, DCPrefix, SegmentMetaPrefix, "200", "20", "2"): mustSegmentValue(t, &datapb.SegmentInfo{ID: 2, CollectionID: 200, PartitionID: 20}),
	}}

	segments, err := ListSegmentsBy(ctx, cli, basePath, SegmentSelector{CollectionID: 100})
	require.NoError(t, err)
	require.Equal(t, []string{path.Join(basePath, DCPrefix, SegmentMetaPrefix, "100") + "/"}, cli.loadedPrefixes)
	require.Empty(t, cli.loadedKeys)
	require.Len(t, segments, 1)
	require.EqualValues(t, 1, segments[0].ID)
}

func TestListSegmentsByFallsBackWhenLeadingHintMissing(t *testing.T) {
	ctx := context.Background()
	basePath := "root"
	cli := &segmentListKV{data: map[string]string{
		path.Join(basePath, DCPrefix, SegmentMetaPrefix, "100", "10", "1"): mustSegmentValue(t, &datapb.SegmentInfo{ID: 1, CollectionID: 100, PartitionID: 10}),
		path.Join(basePath, DCPrefix, SegmentMetaPrefix, "200", "20", "2"): mustSegmentValue(t, &datapb.SegmentInfo{ID: 2, CollectionID: 200, PartitionID: 20}),
	}}

	segments, err := ListSegmentsBy(ctx, cli, basePath, SegmentSelector{SegmentID: 2})
	require.NoError(t, err)
	require.Equal(t, []string{path.Join(basePath, DCPrefix, SegmentMetaPrefix) + "/"}, cli.loadedPrefixes)
	require.Empty(t, cli.loadedKeys)
	require.Len(t, segments, 1)
	require.EqualValues(t, 2, segments[0].ID)
}

func TestListSegmentsByUsesExactKey(t *testing.T) {
	ctx := context.Background()
	basePath := "root"
	segmentKey := path.Join(basePath, DCPrefix, SegmentMetaPrefix, "100", "10", "1")
	cli := &segmentListKV{data: map[string]string{
		segmentKey: mustSegmentValue(t, &datapb.SegmentInfo{ID: 1, CollectionID: 100, PartitionID: 10}),
		path.Join(basePath, DCPrefix, SegmentMetaPrefix, "100", "10", "2"): mustSegmentValue(t, &datapb.SegmentInfo{ID: 2, CollectionID: 100, PartitionID: 10}),
	}}

	segments, err := ListSegmentsBy(ctx, cli, basePath, SegmentSelector{CollectionID: 100, PartitionID: 10, SegmentID: 1})
	require.NoError(t, err)
	require.Empty(t, cli.loadedPrefixes)
	require.Equal(t, []string{segmentKey}, cli.loadedKeys)
	require.Len(t, segments, 1)
	require.EqualValues(t, 1, segments[0].ID)
}

func TestListSegmentsKeepsPostFilters(t *testing.T) {
	ctx := context.Background()
	basePath := "root"
	cli := &segmentListKV{data: map[string]string{
		path.Join(basePath, DCPrefix, SegmentMetaPrefix, "100", "10", "1"): mustSegmentValue(t, &datapb.SegmentInfo{ID: 1, CollectionID: 100, PartitionID: 10, NumOfRows: 10}),
		path.Join(basePath, DCPrefix, SegmentMetaPrefix, "100", "10", "2"): mustSegmentValue(t, &datapb.SegmentInfo{ID: 2, CollectionID: 100, PartitionID: 10, NumOfRows: 20}),
	}}

	segments, err := ListSegments(ctx, cli, basePath, func(segment *models.Segment) bool {
		return segment.NumOfRows == 20
	})
	require.NoError(t, err)
	require.Len(t, segments, 1)
	require.EqualValues(t, 2, segments[0].ID)
}

func TestListSegmentsLazyLoadsBM25Statslogs(t *testing.T) {
	ctx := context.Background()
	basePath := "root"
	cli := &segmentListKV{data: map[string]string{
		path.Join(basePath, DCPrefix, SegmentMetaPrefix, "100", "10", "1"): mustSegmentValue(t, &datapb.SegmentInfo{ID: 1, CollectionID: 100, PartitionID: 10}),
		path.Join(basePath, DCPrefix, SegmentBM25LogPrefix, "100", "10", "1", "101"): mustFieldBinlogValue(t, &datapb.FieldBinlog{
			FieldID: 101,
			Binlogs: []*datapb.Binlog{{
				LogID:      1001,
				LogSize:    10,
				MemorySize: 20,
			}},
		}),
	}}

	segments, err := ListSegments(ctx, cli, basePath)
	require.NoError(t, err)
	require.Len(t, segments, 1)

	bm25Statslogs := segments[0].GetBm25Statslogs()
	require.Len(t, bm25Statslogs, 1)
	require.EqualValues(t, 101, bm25Statslogs[0].FieldID)
	require.Len(t, bm25Statslogs[0].Binlogs, 1)
	require.EqualValues(t, 1001, bm25Statslogs[0].Binlogs[0].LogID)
	require.EqualValues(t, 10, bm25Statslogs[0].Binlogs[0].LogSize)
	require.EqualValues(t, 20, bm25Statslogs[0].Binlogs[0].MemSize)
	require.Equal(t, "ROOT_PATH/bm25_stats/100/10/1/101/1001", bm25Statslogs[0].Binlogs[0].LogPath)
}

func TestListSegmentsKeepsInlineBM25Statslogs(t *testing.T) {
	ctx := context.Background()
	basePath := "root"
	cli := &segmentListKV{data: map[string]string{
		path.Join(basePath, DCPrefix, SegmentMetaPrefix, "100", "10", "1"): mustSegmentValue(t, &datapb.SegmentInfo{
			ID:           1,
			CollectionID: 100,
			PartitionID:  10,
			Bm25Statslogs: []*datapb.FieldBinlog{{
				FieldID: 101,
				Binlogs: []*datapb.Binlog{{
					LogID:      1001,
					LogPath:    "inline-bm25-path",
					LogSize:    10,
					MemorySize: 20,
				}},
			}},
		}),
	}}

	segments, err := ListSegments(ctx, cli, basePath)
	require.NoError(t, err)
	require.Len(t, segments, 1)

	bm25Statslogs := segments[0].GetBm25Statslogs()
	require.Len(t, bm25Statslogs, 1)
	require.Equal(t, "inline-bm25-path", bm25Statslogs[0].Binlogs[0].LogPath)
}

func mustSegmentValue(t *testing.T, info *datapb.SegmentInfo) string {
	t.Helper()

	bs, err := proto.Marshal(info)
	require.NoError(t, err)
	return string(bs)
}

func mustFieldBinlogValue(t *testing.T, info *datapb.FieldBinlog) string {
	t.Helper()

	bs, err := proto.Marshal(info)
	require.NoError(t, err)
	return string(bs)
}
