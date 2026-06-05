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

type snapshotListKV struct {
	kv.MetaKV
	data           map[string]string
	loadedPrefixes []string
	loadedKeys     []string
}

func (s *snapshotListKV) Load(ctx context.Context, key string, opts ...kv.LoadOption) (string, error) {
	s.loadedKeys = append(s.loadedKeys, key)
	value, ok := s.data[key]
	if !ok {
		return "", kv.ErrKeyNotFound
	}
	return value, nil
}

func (s *snapshotListKV) LoadWithPrefix(ctx context.Context, prefix string, opts ...kv.LoadOption) ([]string, []string, error) {
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

func TestListSnapshotsByUsesCollectionPrefix(t *testing.T) {
	ctx := context.Background()
	basePath := "root"
	cli := &snapshotListKV{data: map[string]string{
		path.Join(basePath, DCPrefix, DCSnapshotPrefix, "100", "1"): mustSnapshotValue(t, &datapb.SnapshotInfo{Id: 1, CollectionId: 100}),
		path.Join(basePath, DCPrefix, DCSnapshotPrefix, "200", "2"): mustSnapshotValue(t, &datapb.SnapshotInfo{Id: 2, CollectionId: 200}),
	}}

	snapshots, err := ListSnapshotsBy(ctx, cli, basePath, SnapshotSelector{CollectionID: 100})
	require.NoError(t, err)
	require.Equal(t, []string{path.Join(basePath, DCPrefix, DCSnapshotPrefix, "100") + "/"}, cli.loadedPrefixes)
	require.Empty(t, cli.loadedKeys)
	require.Len(t, snapshots, 1)
	require.EqualValues(t, 1, snapshots[0].GetProto().GetId())
}

func TestListSnapshotsByFallsBackWhenLeadingHintMissing(t *testing.T) {
	ctx := context.Background()
	basePath := "root"
	cli := &snapshotListKV{data: map[string]string{
		path.Join(basePath, DCPrefix, DCSnapshotPrefix, "100", "1"): mustSnapshotValue(t, &datapb.SnapshotInfo{Id: 1, CollectionId: 100}),
		path.Join(basePath, DCPrefix, DCSnapshotPrefix, "200", "2"): mustSnapshotValue(t, &datapb.SnapshotInfo{Id: 2, CollectionId: 200}),
	}}

	snapshots, err := ListSnapshotsBy(ctx, cli, basePath, SnapshotSelector{SnapshotID: 2})
	require.NoError(t, err)
	require.Equal(t, []string{path.Join(basePath, DCPrefix, DCSnapshotPrefix) + "/"}, cli.loadedPrefixes)
	require.Empty(t, cli.loadedKeys)
	require.Len(t, snapshots, 1)
	require.EqualValues(t, 2, snapshots[0].GetProto().GetId())
}

func TestListSnapshotsByUsesExactKey(t *testing.T) {
	ctx := context.Background()
	basePath := "root"
	snapshotKey := path.Join(basePath, DCPrefix, DCSnapshotPrefix, "100", "1")
	cli := &snapshotListKV{data: map[string]string{
		snapshotKey: mustSnapshotValue(t, &datapb.SnapshotInfo{Id: 1, CollectionId: 100}),
		path.Join(basePath, DCPrefix, DCSnapshotPrefix, "100", "2"): mustSnapshotValue(t, &datapb.SnapshotInfo{Id: 2, CollectionId: 100}),
	}}

	snapshots, err := ListSnapshotsBy(ctx, cli, basePath, SnapshotSelector{CollectionID: 100, SnapshotID: 1})
	require.NoError(t, err)
	require.Empty(t, cli.loadedPrefixes)
	require.Equal(t, []string{snapshotKey}, cli.loadedKeys)
	require.Len(t, snapshots, 1)
	require.EqualValues(t, 1, snapshots[0].GetProto().GetId())
}

func TestListSnapshotsKeepsPostFilters(t *testing.T) {
	ctx := context.Background()
	basePath := "root"
	cli := &snapshotListKV{data: map[string]string{
		path.Join(basePath, DCPrefix, DCSnapshotPrefix, "100", "1"): mustSnapshotValue(t, &datapb.SnapshotInfo{Id: 1, CollectionId: 100, Name: "first"}),
		path.Join(basePath, DCPrefix, DCSnapshotPrefix, "100", "2"): mustSnapshotValue(t, &datapb.SnapshotInfo{Id: 2, CollectionId: 100, Name: "second"}),
	}}

	snapshots, err := ListSnapshots(ctx, cli, basePath, func(snapshot *models.Snapshot) bool {
		return snapshot.GetProto().GetName() == "second"
	})
	require.NoError(t, err)
	require.Len(t, snapshots, 1)
	require.EqualValues(t, 2, snapshots[0].GetProto().GetId())
}

func mustSnapshotValue(t *testing.T, info *datapb.SnapshotInfo) string {
	t.Helper()

	bs, err := proto.Marshal(info)
	require.NoError(t, err)
	return string(bs)
}
