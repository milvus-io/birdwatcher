package common

import (
	"context"
	"path"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
)

type rootCoordListKV struct {
	kv.MetaKV
	data           map[string]string
	loadedPrefixes []string
	loadedKeys     []string
}

func (r *rootCoordListKV) Load(ctx context.Context, key string, opts ...kv.LoadOption) (string, error) {
	r.loadedKeys = append(r.loadedKeys, key)
	value, ok := r.data[key]
	if !ok {
		return "", kv.ErrKeyNotFound
	}
	return value, nil
}

func (r *rootCoordListKV) LoadWithPrefix(ctx context.Context, prefix string, opts ...kv.LoadOption) ([]string, []string, error) {
	r.loadedPrefixes = append(r.loadedPrefixes, prefix)

	keys := make([]string, 0)
	for key := range r.data {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)

	values := make([]string, 0, len(keys))
	for _, key := range keys {
		values = append(values, r.data[key])
	}
	return keys, values, nil
}

func TestListDatabaseByUsesExactKey(t *testing.T) {
	ctx := context.Background()
	basePath := "root"
	dbKey := path.Join(basePath, RCPrefix, DBPrefix, DBInfoPrefix, "1")
	cli := &rootCoordListKV{data: map[string]string{
		dbKey: mustDatabaseValue(t, &etcdpb.DatabaseInfo{Id: 1, Name: "default"}),
		path.Join(basePath, RCPrefix, DBPrefix, DBInfoPrefix, "2"): mustDatabaseValue(t, &etcdpb.DatabaseInfo{Id: 2, Name: "tenant"}),
	}}

	dbs, err := ListDatabaseBy(ctx, cli, basePath, DatabaseSelector{DatabaseID: 1})
	require.NoError(t, err)
	require.Empty(t, cli.loadedPrefixes)
	require.Equal(t, []string{dbKey}, cli.loadedKeys)
	require.Len(t, dbs, 1)
	require.EqualValues(t, 1, dbs[0].GetProto().GetId())
}

func TestListDatabaseByKeepsNameFilter(t *testing.T) {
	ctx := context.Background()
	basePath := "root"
	cli := &rootCoordListKV{data: map[string]string{
		path.Join(basePath, RCPrefix, DBPrefix, DBInfoPrefix, "1"): mustDatabaseValue(t, &etcdpb.DatabaseInfo{Id: 1, Name: "default"}),
		path.Join(basePath, RCPrefix, DBPrefix, DBInfoPrefix, "2"): mustDatabaseValue(t, &etcdpb.DatabaseInfo{Id: 2, Name: "tenant"}),
	}}

	dbs, err := ListDatabaseBy(ctx, cli, basePath, DatabaseSelector{DatabaseName: "tenant"})
	require.NoError(t, err)
	require.Equal(t, []string{path.Join(basePath, RCPrefix, DBPrefix, DBInfoPrefix) + "/"}, cli.loadedPrefixes)
	require.Empty(t, cli.loadedKeys)
	require.Len(t, dbs, 1)
	require.Equal(t, "tenant", dbs[0].GetProto().GetName())
}

func TestListCollectionWithoutFieldsByUsesLegacyExactAndDBFallback(t *testing.T) {
	ctx := context.Background()
	basePath := "root"
	legacyKey := path.Join(basePath, CollectionMetaPrefix, "100")
	cli := &rootCoordListKV{data: map[string]string{
		legacyKey: mustCollectionValue(t, 100, 0, "legacy"),
		path.Join(basePath, DBCollectionMetaPrefix, "1", "100"): mustCollectionValue(t, 100, 1, "db"),
		path.Join(basePath, DBCollectionMetaPrefix, "2", "200"): mustCollectionValue(t, 200, 2, "other"),
	}}

	collections, err := ListCollectionWithoutFieldsBy(ctx, cli, basePath, CollectionSelector{CollectionID: 100})
	require.NoError(t, err)
	require.Equal(t, []string{legacyKey}, cli.loadedKeys)
	require.Equal(t, []string{path.Join(basePath, DBCollectionMetaPrefix) + "/"}, cli.loadedPrefixes)
	require.Len(t, collections, 2)
	require.ElementsMatch(t, []int64{0, 1}, []int64{collections[0].GetProto().GetDbId(), collections[1].GetProto().GetDbId()})
}

func TestListCollectionWithoutFieldsByUsesDBExactWhenDatabaseKnown(t *testing.T) {
	ctx := context.Background()
	basePath := "root"
	legacyKey := path.Join(basePath, CollectionMetaPrefix, "100")
	dbKey := path.Join(basePath, DBCollectionMetaPrefix, "1", "100")
	cli := &rootCoordListKV{data: map[string]string{
		dbKey: mustCollectionValue(t, 100, 1, "db"),
		path.Join(basePath, DBCollectionMetaPrefix, "1", "200"): mustCollectionValue(t, 200, 1, "other"),
	}}

	collections, err := ListCollectionWithoutFieldsBy(ctx, cli, basePath, CollectionSelector{DatabaseID: 1, UseDatabaseID: true, CollectionID: 100})
	require.NoError(t, err)
	require.Equal(t, []string{legacyKey, dbKey}, cli.loadedKeys)
	require.Empty(t, cli.loadedPrefixes)
	require.Len(t, collections, 1)
	require.EqualValues(t, 100, collections[0].GetProto().GetID())
	require.EqualValues(t, 1, collections[0].GetProto().GetDbId())
}

func TestListCollectionWithoutFieldsByUsesDBPrefix(t *testing.T) {
	ctx := context.Background()
	basePath := "root"
	cli := &rootCoordListKV{data: map[string]string{
		path.Join(basePath, DBCollectionMetaPrefix, "1", "100"): mustCollectionValue(t, 100, 1, "db"),
		path.Join(basePath, DBCollectionMetaPrefix, "2", "200"): mustCollectionValue(t, 200, 2, "other"),
	}}

	collections, err := ListCollectionWithoutFieldsBy(ctx, cli, basePath, CollectionSelector{DatabaseID: 1, UseDatabaseID: true})
	require.NoError(t, err)
	require.Equal(t, []string{
		path.Join(basePath, CollectionMetaPrefix) + "/",
		path.Join(basePath, DBCollectionMetaPrefix, "1") + "/",
	}, cli.loadedPrefixes)
	require.Empty(t, cli.loadedKeys)
	require.Len(t, collections, 1)
	require.EqualValues(t, 1, collections[0].GetProto().GetDbId())
}

func TestListPartitionsByUsesCollectionPrefix(t *testing.T) {
	ctx := context.Background()
	basePath := "root"
	cli := &rootCoordListKV{data: map[string]string{
		path.Join(basePath, RCPrefix, PartitionPrefix, "100", "10"): mustPartitionValue(t, 100, 10, "p1"),
		path.Join(basePath, RCPrefix, PartitionPrefix, "200", "20"): mustPartitionValue(t, 200, 20, "p2"),
	}}

	partitions, err := ListPartitionsBy(ctx, cli, basePath, PartitionSelector{CollectionID: 100})
	require.NoError(t, err)
	require.Equal(t, []string{path.Join(basePath, RCPrefix, PartitionPrefix, "100") + "/"}, cli.loadedPrefixes)
	require.Empty(t, cli.loadedKeys)
	require.Len(t, partitions, 1)
	require.EqualValues(t, 10, partitions[0].GetProto().GetPartitionID())
}

func TestListPartitionsByUsesExactKey(t *testing.T) {
	ctx := context.Background()
	basePath := "root"
	partitionKey := path.Join(basePath, RCPrefix, PartitionPrefix, "100", "10")
	cli := &rootCoordListKV{data: map[string]string{
		partitionKey: mustPartitionValue(t, 100, 10, "p1"),
		path.Join(basePath, RCPrefix, PartitionPrefix, "100", "11"): mustPartitionValue(t, 100, 11, "p2"),
	}}

	partitions, err := ListPartitionsBy(ctx, cli, basePath, PartitionSelector{CollectionID: 100, PartitionID: 10})
	require.NoError(t, err)
	require.Empty(t, cli.loadedPrefixes)
	require.Equal(t, []string{partitionKey}, cli.loadedKeys)
	require.Len(t, partitions, 1)
	require.EqualValues(t, 10, partitions[0].GetProto().GetPartitionID())
}

func TestListPartitionsByFallsBackWithoutCollectionID(t *testing.T) {
	ctx := context.Background()
	basePath := "root"
	cli := &rootCoordListKV{data: map[string]string{
		path.Join(basePath, RCPrefix, PartitionPrefix, "100", "10"): mustPartitionValue(t, 100, 10, "p1"),
		path.Join(basePath, RCPrefix, PartitionPrefix, "200", "20"): mustPartitionValue(t, 200, 20, "p2"),
	}}

	partitions, err := ListPartitionsBy(ctx, cli, basePath, PartitionSelector{PartitionID: 20})
	require.NoError(t, err)
	require.Equal(t, []string{path.Join(basePath, RCPrefix, PartitionPrefix) + "/"}, cli.loadedPrefixes)
	require.Empty(t, cli.loadedKeys)
	require.Len(t, partitions, 1)
	require.EqualValues(t, 20, partitions[0].GetProto().GetPartitionID())
}

func mustDatabaseValue(t *testing.T, info *etcdpb.DatabaseInfo) string {
	t.Helper()

	bs, err := proto.Marshal(info)
	require.NoError(t, err)
	return string(bs)
}

func mustCollectionValue(t *testing.T, collectionID int64, dbID int64, name string) string {
	t.Helper()

	bs, err := proto.Marshal(&etcdpb.CollectionInfo{
		ID:   collectionID,
		DbId: dbID,
		Schema: &schemapb.CollectionSchema{
			Name: name,
		},
	})
	require.NoError(t, err)
	return string(bs)
}

func mustPartitionValue(t *testing.T, collectionID int64, partitionID int64, name string) string {
	t.Helper()

	bs, err := proto.Marshal(&etcdpb.PartitionInfo{
		CollectionId:  collectionID,
		PartitionID:   partitionID,
		PartitionName: name,
	})
	require.NoError(t, err)
	return string(bs)
}
