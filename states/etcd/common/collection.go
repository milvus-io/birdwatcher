package common

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/samber/lo"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/proto/v2.0/etcdpb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/schemapb"
	etcdpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/etcdpb"
	schemapbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/schemapb"
	"github.com/milvus-io/birdwatcher/states/kv"
)

const (
	SnapshotPrefix = "snapshots"
	// CollectionMetaPrefix is prefix for rootcoord collection meta.
	CollectionMetaPrefix = `root-coord/collection`
	// DBCollectionMetaPrefix is prefix for rootcoord database collection meta
	DBCollectionMetaPrefix = `root-coord/database/collection-info`
	// FieldMetaPrefix is prefix for rootcoord collection fields meta
	FieldMetaPrefix = `root-coord/fields`
	// CollectionLoadPrefix is prefix for querycoord collection loaded in milvus v2.1.x
	CollectionLoadPrefix = "queryCoord-collectionMeta"
	// CollectionLoadPrefixV2 is prefix for querycoord collection loaded in milvus v2.2.x
	CollectionLoadPrefixV2      = "querycoord-collection-loadinfo"
	PartitionLoadedPrefixLegacy = "queryCoord-partitionMeta"
	PartitionLoadedPrefix       = "querycoord-partition-loadinfo"

	CompactionTaskPrefix = "datacoord-meta/compaction-task"
)

var (
	// ErrCollectionDropped sample error for collection dropped.
	ErrCollectionDropped = errors.New("collection dropped")
	// ErrCollectionNotFound sample error for collection not found.
	ErrCollectionNotFound = errors.New("collection not found")
	// CollectionTombstone is the special mark for collection dropped.
	CollectionTombstone = []byte{0xE2, 0x9B, 0xBC}
)

// ListCollections returns collection information.
// the field info might not include.
func ListCollections(cli kv.MetaKV, basePath string, filter func(*etcdpb.CollectionInfo) bool) ([]etcdpb.CollectionInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	colls, _, err := ListProtoObjectsAdv(ctx, cli, path.Join(basePath, CollectionMetaPrefix), func(_ string, value []byte) bool {
		return !bytes.Equal(value, CollectionTombstone)
	}, filter)
	return colls, err
}

// ListCollectionsVersion returns collection information as provided version.
func ListCollectionsVersion(ctx context.Context, cli kv.MetaKV, basePath string, version string, filters ...func(*models.Collection) bool) ([]*models.Collection, error) {
	prefixes := []string{
		path.Join(basePath, CollectionMetaPrefix),
		path.Join(basePath, DBCollectionMetaPrefix),
	}
	var result []*models.Collection
	switch version {
	case models.LTEVersion2_1:
		for _, prefix := range prefixes {
			collections, keys, err := ListProtoObjectsAdv[etcdpb.CollectionInfo](ctx, cli, prefix, func(_ string, value []byte) bool {
				// TODO maybe add dropped collection info in result?
				return !bytes.Equal(value, CollectionTombstone)
			})
			if err != nil {
				return nil, err
			}
			result = append(result, lo.FilterMap(collections, func(collection etcdpb.CollectionInfo, idx int) (*models.Collection, bool) {
				c := models.NewCollectionFromV2_1(&collection, keys[idx])
				for _, filter := range filters {
					if !filter(c) {
						return nil, false
					}
				}
				return c, true
			})...)
		}

		return result, nil
	case models.GTEVersion2_2:
		for _, prefix := range prefixes {
			collections, keys, err := ListProtoObjectsAdv[etcdpbv2.CollectionInfo](ctx, cli, prefix, func(_ string, value []byte) bool {
				return !bytes.Equal(value, CollectionTombstone)
			})
			if err != nil {
				return nil, err
			}
			result = append(result, lo.FilterMap(collections, func(collection etcdpbv2.CollectionInfo, idx int) (*models.Collection, bool) {
				fields, err := getCollectionFields(ctx, cli, basePath, collection.ID)
				if err != nil {
					fmt.Println(err.Error())
					return nil, false
				}
				c := models.NewCollectionFromV2_2(&collection, keys[idx], fields)
				for _, filter := range filters {
					if !filter(c) {
						return nil, false
					}
				}
				return c, true
			})...)
		}
		return result, nil
	default:
		return nil, fmt.Errorf("undefined version: %s", version)
	}
}

// GetCollectionByIDVersion retruns collection info from etcd with provided version & id.
func GetCollectionByIDVersion(ctx context.Context, cli kv.MetaKV, basePath string, version string, collID int64) (*models.Collection, error) {
	var ck string
	var cv []byte
	found := false

	prefix := path.Join(basePath, CollectionMetaPrefix, strconv.FormatInt(collID, 10))
	val, err := cli.Load(ctx, prefix)
	if err != nil {
		fmt.Println("get error", err.Error())
		return nil, err
	}
	if len(val) > 0 {
		found = true
		ck = prefix
		cv = []byte(val)
	}

	// with database, dbID unknown here
	prefix = path.Join(basePath, DBCollectionMetaPrefix)
	keys, _, _ := cli.LoadWithPrefix(ctx, prefix)
	suffix := strconv.FormatInt(collID, 10)
	for _, key := range keys {
		if strings.HasSuffix(key, suffix) {
			if found {
				return nil, fmt.Errorf("multiple key found for collection %d: %s, %s", collID, ck, key)
			}
			found = true
			ck = prefix
			cv = []byte(val)
		}
	}
	if !found {
		return nil, fmt.Errorf("collection %d not found in etcd %w", collID, ErrCollectionNotFound)
	}

	if bytes.Equal(cv, CollectionTombstone) {
		return nil, fmt.Errorf("%w, collection id: %d", ErrCollectionDropped, collID)
	}

	switch version {
	case models.LTEVersion2_1:
		info := &etcdpb.CollectionInfo{}
		err := proto.Unmarshal(cv, info)
		if err != nil {
			return nil, err
		}
		c := models.NewCollectionFromV2_1(info, ck)
		return c, nil

	case models.GTEVersion2_2:
		info := &etcdpbv2.CollectionInfo{}
		err := proto.Unmarshal(cv, info)
		if err != nil {
			return nil, err
		}
		fields, err := getCollectionFields(ctx, cli, basePath, info.ID)
		if err != nil {
			return nil, err
		}
		c := models.NewCollectionFromV2_2(info, ck, fields)
		return c, nil
	default:
		return nil, errors.New("not supported version")
	}
}

func getCollectionFields(ctx context.Context, cli kv.MetaKV, basePath string, collID int64) ([]*schemapbv2.FieldSchema, error) {
	fields, _, err := ListProtoObjects[schemapbv2.FieldSchema](ctx, cli, path.Join(basePath, fmt.Sprintf("root-coord/fields/%d", collID)))
	if err != nil {
		fmt.Println(err.Error())
	}
	return lo.Map(fields, func(field schemapbv2.FieldSchema, _ int) *schemapbv2.FieldSchema { return &field }), nil
}

func FillFieldSchemaIfEmpty(cli kv.MetaKV, basePath string, collection *etcdpb.CollectionInfo) error {
	if len(collection.GetSchema().GetFields()) == 0 { // fields separated from schema after 2.1.1
		keys, vals, err := cli.LoadWithPrefix(context.TODO(), path.Join(basePath, fmt.Sprintf("root-coord/fields/%d", collection.ID)))
		if err != nil {
			return err
		}
		if len(keys) != len(vals) {
			return fmt.Errorf("error: keys and vals of different size:%d vs %d", len(keys), len(vals))
		}
		for i, key := range keys {
			field := &schemapb.FieldSchema{}
			err := proto.Unmarshal([]byte(vals[i]), field)
			if err != nil {
				fmt.Println("found error field:", key, err.Error())
				continue
			}
			collection.Schema.Fields = append(collection.Schema.Fields, field)
		}
	}

	return nil
}

func FillFieldSchemaIfEmptyV2(cli kv.MetaKV, basePath string, collection *etcdpbv2.CollectionInfo) error {
	if len(collection.GetSchema().GetFields()) == 0 { // fields separated from schema after 2.1.1
		keys, vals, err := cli.LoadWithPrefix(context.TODO(), fmt.Sprintf("root-coord/fields/%d", collection.ID))
		if err != nil {
			return err
		}
		if len(keys) != len(vals) {
			return fmt.Errorf("error: keys and vals of different size:%d vs %d", len(keys), len(vals))
		}
		for i, key := range keys {
			field := &schemapbv2.FieldSchema{}
			err := proto.Unmarshal([]byte(vals[i]), field)
			if err != nil {
				fmt.Println("found error field:", key, err.Error())
				continue
			}
			collection.Schema.Fields = append(collection.Schema.Fields, field)
		}
	}

	return nil
}

func UpdateCollection(ctx context.Context, cli kv.MetaKV, basePath string, collectionID int64, fn func(coll *etcdpbv2.CollectionInfo), dryRun bool) error {
	prefix := path.Join(basePath, CollectionMetaPrefix, strconv.FormatInt(collectionID, 10))
	val, err := cli.Load(ctx, prefix)
	if err != nil {
		return err
	}

	info := &etcdpbv2.CollectionInfo{}
	err = proto.Unmarshal([]byte(val), info)
	if err != nil {
		return err
	}

	fn(info)

	bs, err := proto.Marshal(info)
	if err != nil {
		return err
	}

	if dryRun {
		fmt.Println("dry run")
		fmt.Println("before alter")
		fmt.Printf("schema:%s", info.Schema.String())
		fmt.Println("after alter")
		fmt.Printf("schema:%s", info.Schema.String())
		return nil
	}

	err = cli.Save(ctx, prefix, string(bs))
	return err
}

func UpdateField(ctx context.Context, cli kv.MetaKV, basePath string, collectionID, fieldID int64, fn func(field *schemapbv2.FieldSchema), dryRun bool) error {
	prefix := path.Join(basePath, SnapshotPrefix, FieldMetaPrefix, strconv.FormatInt(collectionID, 10))
	keys, values, err := cli.LoadWithPrefix(ctx, prefix)
	if err != nil {
		return err
	}

	if len(keys) <= 0 {
		return fmt.Errorf("wrong path %s", prefix)
	}

	matchedKey := ""
	var matchedValue []byte
	curTs := int64(0)
	for idx, key := range keys {
		baseName := path.Base(key)
		parts := strings.Split(baseName, "_")
		i, err2 := strconv.Atoi(parts[0])
		if err2 != nil {
			return err2
		}

		if int64(i) == fieldID {
			tsString := parts[1][2:]
			ts, err := strconv.Atoi(tsString)
			if err != nil {
				return nil
			}
			ts2 := int64(ts)
			if ts2 > curTs {
				curTs = ts2
				matchedKey = key
				matchedValue = []byte(values[idx])
			}
		}
	}

	if len(matchedValue) == 0 || len(matchedKey) == 0 {
		return fmt.Errorf("not found field")
	}
	fmt.Println("matchedKey:", matchedKey)
	info := &schemapbv2.FieldSchema{}
	err = proto.Unmarshal(matchedValue, info)
	if err != nil {
		return err
	}
	fmt.Println("before alter", info)
	fn(info)
	bs, err := proto.Marshal(info)
	if err != nil {
		return err
	}

	if dryRun {
		fmt.Printf("try alter field schema:%s\n", info.String())
		return nil
	}
	err = cli.Save(ctx, matchedKey, string(bs))
	if err != nil {
		return err
	}
	fmt.Printf("alter field schema:%s\n", info.String())

	prefixCache := path.Join(basePath, FieldMetaPrefix, strconv.FormatInt(collectionID, 10), strconv.FormatInt(fieldID, 10))
	_, err = cli.Load(ctx, prefixCache)
	if err != nil {
		fmt.Println("no need save to cache")
		return nil
	}
	err = cli.Save(ctx, prefixCache, string(bs))
	if err != nil {
		return err
	}
	fmt.Printf("alter field schema cache :%s\n", info.String())
	return nil
}
