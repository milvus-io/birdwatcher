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
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/proto/v2.0/etcdpb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/schemapb"
	etcdpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/etcdpb"
	schemapbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/schemapb"
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
func ListCollections(cli clientv3.KV, basePath string, filter func(*etcdpb.CollectionInfo) bool) ([]etcdpb.CollectionInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	colls, _, err := ListProtoObjectsAdv(ctx, cli, path.Join(basePath, CollectionMetaPrefix), func(_ string, value []byte) bool {
		return !bytes.Equal(value, CollectionTombstone)
	}, filter)
	return colls, err
}

// ListCollectionsVersion returns collection information as provided version.
func ListCollectionsVersion(ctx context.Context, cli clientv3.KV, basePath string, version string, filters ...func(*models.Collection) bool) ([]*models.Collection, error) {
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
func GetCollectionByIDVersion(ctx context.Context, cli clientv3.KV, basePath string, version string, collID int64) (*models.Collection, error) {
	var result []*mvccpb.KeyValue

	// meta before database
	prefix := path.Join(basePath, CollectionMetaPrefix, strconv.FormatInt(collID, 10))
	resp, err := cli.Get(ctx, prefix)
	if err != nil {
		fmt.Println("get error", err.Error())
		return nil, err
	}
	result = append(result, resp.Kvs...)

	// with database, dbID unknown here
	prefix = path.Join(basePath, DBCollectionMetaPrefix)
	resp, _ = cli.Get(ctx, prefix, clientv3.WithPrefix())
	suffix := strconv.FormatInt(collID, 10)
	for _, kv := range resp.Kvs {
		if strings.HasSuffix(string(kv.Key), suffix) {
			result = append(result, kv)
		}
	}

	if len(result) != 1 {
		return nil, fmt.Errorf("collection %d not found in etcd %w", collID, ErrCollectionNotFound)
	}

	kv := result[0]

	if bytes.Equal(kv.Value, CollectionTombstone) {
		return nil, fmt.Errorf("%w, collection id: %d", ErrCollectionDropped, collID)
	}

	switch version {
	case models.LTEVersion2_1:
		info := &etcdpb.CollectionInfo{}
		err := proto.Unmarshal(kv.Value, info)
		if err != nil {
			return nil, err
		}
		c := models.NewCollectionFromV2_1(info, string(kv.Key))
		return c, nil

	case models.GTEVersion2_2:
		info := &etcdpbv2.CollectionInfo{}
		err := proto.Unmarshal(kv.Value, info)
		if err != nil {
			return nil, err
		}
		fields, err := getCollectionFields(ctx, cli, basePath, info.ID)
		if err != nil {
			return nil, err
		}
		c := models.NewCollectionFromV2_2(info, string(kv.Key), fields)
		return c, nil
	default:
		return nil, errors.New("not supported version")
	}
}

func getCollectionFields(ctx context.Context, cli clientv3.KV, basePath string, collID int64) ([]*schemapbv2.FieldSchema, error) {
	fields, _, err := ListProtoObjects[schemapbv2.FieldSchema](ctx, cli, path.Join(basePath, fmt.Sprintf("root-coord/fields/%d", collID)))
	if err != nil {
		fmt.Println(err.Error())
	}
	return lo.Map(fields, func(field schemapbv2.FieldSchema, _ int) *schemapbv2.FieldSchema { return &field }), nil
}

func FillFieldSchemaIfEmpty(cli clientv3.KV, basePath string, collection *etcdpb.CollectionInfo) error {
	if len(collection.GetSchema().GetFields()) == 0 { // fields separated from schema after 2.1.1
		resp, err := cli.Get(context.TODO(), path.Join(basePath, fmt.Sprintf("root-coord/fields/%d", collection.ID)), clientv3.WithPrefix())
		if err != nil {
			return err
		}
		for _, kv := range resp.Kvs {
			field := &schemapb.FieldSchema{}
			err := proto.Unmarshal(kv.Value, field)
			if err != nil {
				fmt.Println("found error field:", string(kv.Key), err.Error())
				continue
			}
			collection.Schema.Fields = append(collection.Schema.Fields, field)
		}
	}

	return nil
}

func FillFieldSchemaIfEmptyV2(cli clientv3.KV, basePath string, collection *etcdpbv2.CollectionInfo) error {
	if len(collection.GetSchema().GetFields()) == 0 { // fields separated from schema after 2.1.1
		resp, err := cli.Get(context.TODO(), path.Join(basePath, fmt.Sprintf("root-coord/fields/%d", collection.ID)), clientv3.WithPrefix())
		if err != nil {
			return err
		}
		for _, kv := range resp.Kvs {
			field := &schemapbv2.FieldSchema{}
			err := proto.Unmarshal(kv.Value, field)
			if err != nil {
				fmt.Println("found error field:", string(kv.Key), err.Error())
				continue
			}
			collection.Schema.Fields = append(collection.Schema.Fields, field)
		}
	}

	return nil
}

func UpdateCollection(ctx context.Context, cli clientv3.KV, basePath string, collectionID int64, fn func(coll *etcdpbv2.CollectionInfo)) error {
	prefix := path.Join(basePath, CollectionMetaPrefix, strconv.FormatInt(collectionID, 10))
	resp, err := cli.Get(ctx, prefix)
	if err != nil {
		return err
	}
	info := &etcdpbv2.CollectionInfo{}
	err = proto.Unmarshal(resp.Kvs[0].Value, info)
	if err != nil {
		return err
	}

	fn(info)

	bs, err := proto.Marshal(info)
	if err != nil {
		return err
	}

	_, err = cli.Put(ctx, prefix, string(bs))
	return err
}
