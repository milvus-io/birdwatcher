package common

import (
	"context"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func TestListCollectionsByUsesIndividualEnrichmentAtDefaultThreshold(t *testing.T) {
	ctx := context.Background()
	basePath := "root"
	functionKey := path.Join(basePath, FunctionMetaPrefix, "100", "30")
	cli := &rootCoordListKV{data: map[string]string{
		path.Join(basePath, CollectionMetaPrefix, "100"):             mustCollectionValue(t, 100, 0, "coll100"),
		path.Join(basePath, FieldMetaPrefix, "100", "10"):            mustFieldSchemaValue(t, 10, "field10"),
		path.Join(basePath, StructArrayFieldMetaPrefix, "100", "20"): mustStructArrayFieldSchemaValue(t, 20, "struct20"),
		functionKey: mustFunctionSchemaValue(t, 30, "function30"),
	}}

	collections, err := ListCollectionsBy(ctx, cli, basePath, CollectionSelector{CollectionID: 100})
	require.NoError(t, err)
	require.Len(t, collections, 1)
	require.Len(t, collections[0].GetProto().GetSchema().GetFields(), 1)
	require.Equal(t, "field10", collections[0].GetProto().GetSchema().GetFields()[0].GetName())
	require.Len(t, collections[0].GetProto().GetSchema().GetStructArrayFields(), 1)
	require.Equal(t, "struct20", collections[0].GetProto().GetSchema().GetStructArrayFields()[0].GetName())
	require.Len(t, collections[0].Functions, 1)
	require.Equal(t, functionKey, collections[0].Functions[0].Key())
	require.Equal(t, 1, prefixCount(cli.loadedPrefixes, path.Join(basePath, FieldMetaPrefix, "100")))
	require.Equal(t, 1, prefixCount(cli.loadedPrefixes, path.Join(basePath, StructArrayFieldMetaPrefix, "100")))
	require.Equal(t, 1, prefixCount(cli.loadedPrefixes, path.Join(basePath, FunctionMetaPrefix, "100")+"/"))
	require.Equal(t, 0, prefixCount(cli.loadedPrefixes, path.Join(basePath, FieldMetaPrefix)+"/"))
	require.Equal(t, 0, prefixCount(cli.loadedPrefixes, path.Join(basePath, StructArrayFieldMetaPrefix)+"/"))
	require.Equal(t, 0, prefixCount(cli.loadedPrefixes, path.Join(basePath, FunctionMetaPrefix)+"/"))
}

func TestListCollectionsByBulkEnrichesAboveDefaultThreshold(t *testing.T) {
	ctx := context.Background()
	basePath := "root"
	function100Key := path.Join(basePath, FunctionMetaPrefix, "100", "30")
	function200Key := path.Join(basePath, FunctionMetaPrefix, "200", "60")
	cli := &rootCoordListKV{data: map[string]string{
		path.Join(basePath, CollectionMetaPrefix, "100"):             mustCollectionValue(t, 100, 0, "coll100"),
		path.Join(basePath, CollectionMetaPrefix, "200"):             mustCollectionValue(t, 200, 0, "coll200"),
		path.Join(basePath, FieldMetaPrefix, "100", "10"):            mustFieldSchemaValue(t, 10, "field100"),
		path.Join(basePath, FieldMetaPrefix, "200", "20"):            mustFieldSchemaValue(t, 20, "field200"),
		path.Join(basePath, FieldMetaPrefix, "300", "30"):            mustFieldSchemaValue(t, 30, "field300"),
		path.Join(basePath, StructArrayFieldMetaPrefix, "100", "40"): mustStructArrayFieldSchemaValue(t, 40, "struct100"),
		path.Join(basePath, StructArrayFieldMetaPrefix, "200", "50"): mustStructArrayFieldSchemaValue(t, 50, "struct200"),
		function100Key: mustFunctionSchemaValue(t, 30, "function100"),
		function200Key: mustFunctionSchemaValue(t, 60, "function200"),
		path.Join(basePath, FunctionMetaPrefix, "300", "90"): mustFunctionSchemaValue(t, 90, "function300"),
	}}

	collections, err := ListCollectionsBy(ctx, cli, basePath, CollectionSelector{})
	require.NoError(t, err)
	byID := collectionsByID(collections)
	require.Len(t, byID, 2)
	require.Equal(t, "field100", byID[100].GetProto().GetSchema().GetFields()[0].GetName())
	require.Equal(t, "field200", byID[200].GetProto().GetSchema().GetFields()[0].GetName())
	require.Equal(t, "struct100", byID[100].GetProto().GetSchema().GetStructArrayFields()[0].GetName())
	require.Equal(t, "struct200", byID[200].GetProto().GetSchema().GetStructArrayFields()[0].GetName())
	require.Equal(t, function100Key, byID[100].Functions[0].Key())
	require.Equal(t, function200Key, byID[200].Functions[0].Key())
	require.Equal(t, 1, prefixCount(cli.loadedPrefixes, path.Join(basePath, FieldMetaPrefix)+"/"))
	require.Equal(t, 1, prefixCount(cli.loadedPrefixes, path.Join(basePath, StructArrayFieldMetaPrefix)+"/"))
	require.Equal(t, 1, prefixCount(cli.loadedPrefixes, path.Join(basePath, FunctionMetaPrefix)+"/"))
	require.Equal(t, 0, prefixCount(cli.loadedPrefixes, path.Join(basePath, FieldMetaPrefix, "100")))
	require.Equal(t, 0, prefixCount(cli.loadedPrefixes, path.Join(basePath, StructArrayFieldMetaPrefix, "100")))
	require.Equal(t, 0, prefixCount(cli.loadedPrefixes, path.Join(basePath, FunctionMetaPrefix, "100")+"/"))
}

func TestListCollectionsByHonorsBulkEnrichmentThreshold(t *testing.T) {
	ctx := context.Background()
	basePath := "root"
	cli := &rootCoordListKV{data: map[string]string{
		path.Join(basePath, CollectionMetaPrefix, "100"):  mustCollectionValue(t, 100, 0, "coll100"),
		path.Join(basePath, CollectionMetaPrefix, "200"):  mustCollectionValue(t, 200, 0, "coll200"),
		path.Join(basePath, FieldMetaPrefix, "100", "10"): mustFieldSchemaValue(t, 10, "field100"),
		path.Join(basePath, FieldMetaPrefix, "200", "20"): mustFieldSchemaValue(t, 20, "field200"),
	}}

	collections, err := ListCollectionsBy(ctx, cli, basePath, CollectionSelector{BulkEnrichmentThreshold: intPtr(2)})
	require.NoError(t, err)
	require.Len(t, collections, 2)
	require.Equal(t, 1, prefixCount(cli.loadedPrefixes, path.Join(basePath, FieldMetaPrefix, "100")))
	require.Equal(t, 1, prefixCount(cli.loadedPrefixes, path.Join(basePath, FieldMetaPrefix, "200")))
	require.Equal(t, 0, prefixCount(cli.loadedPrefixes, path.Join(basePath, FieldMetaPrefix)+"/"))
}

func TestListCollectionsByBulkEnrichmentSkipsMalformedAndUnwantedEntries(t *testing.T) {
	ctx := context.Background()
	basePath := "root"
	cli := &rootCoordListKV{data: map[string]string{
		path.Join(basePath, CollectionMetaPrefix, "100"):  mustCollectionValue(t, 100, 0, "coll100"),
		path.Join(basePath, CollectionMetaPrefix, "200"):  mustCollectionValue(t, 200, 0, "coll200"),
		path.Join(basePath, FieldMetaPrefix, "100", "10"): mustFieldSchemaValue(t, 10, "field100"),
		path.Join(basePath, FieldMetaPrefix, "300", "30"): mustFieldSchemaValue(t, 30, "field300"),
		path.Join(basePath, FieldMetaPrefix, "bad", "40"): mustFieldSchemaValue(t, 40, "fieldBad"),
		path.Join(basePath, FieldMetaPrefix, "100", "50"): string([]byte{0xff}),
	}}

	collections, err := ListCollectionsBy(ctx, cli, basePath, CollectionSelector{})
	require.NoError(t, err)
	byID := collectionsByID(collections)
	require.Len(t, byID[100].GetProto().GetSchema().GetFields(), 1)
	require.Equal(t, "field100", byID[100].GetProto().GetSchema().GetFields()[0].GetName())
	require.Empty(t, byID[200].GetProto().GetSchema().GetFields())
}

func mustFieldSchemaValue(t *testing.T, id int64, name string) string {
	t.Helper()

	bs, err := proto.Marshal(&schemapb.FieldSchema{FieldID: id, Name: name})
	require.NoError(t, err)
	return string(bs)
}

func mustStructArrayFieldSchemaValue(t *testing.T, id int64, name string) string {
	t.Helper()

	bs, err := proto.Marshal(&schemapb.StructArrayFieldSchema{FieldID: id, Name: name})
	require.NoError(t, err)
	return string(bs)
}

func mustFunctionSchemaValue(t *testing.T, id int64, name string) string {
	t.Helper()

	bs, err := proto.Marshal(&schemapb.FunctionSchema{Id: id, Name: name})
	require.NoError(t, err)
	return string(bs)
}

func intPtr(v int) *int {
	return &v
}

func prefixCount(prefixes []string, target string) int {
	count := 0
	for _, prefix := range prefixes {
		if prefix == target {
			count++
		}
	}
	return count
}

func collectionsByID(collections []*models.Collection) map[int64]*models.Collection {
	result := make(map[int64]*models.Collection, len(collections))
	for _, collection := range collections {
		result[collection.GetProto().GetID()] = collection
	}
	return result
}
