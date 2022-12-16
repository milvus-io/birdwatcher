package common

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path"
	"strconv"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/birdwatcher/proto/v2.0/etcdpb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/querypb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/schemapb"
	querypbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/querypb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	// CollectionLoadPrefix is prefix for querycoord collection loaded in milvus v2.1.x
	CollectionLoadPrefix = "queryCoord-collectionMeta"
	// CollectionLoadPrefixV2 is prefix for querycoord collection loaded in milvus v2.2.x
	CollectionLoadPrefixV2 = "querycoord-collection-loadinfo"
)

var (
	ErrCollectionDropped = errors.New("collection dropped")
	// CollectionTombstone is the special mark for collection dropped.
	CollectionTombstone = []byte{0xE2, 0x9B, 0xBC}
)

// GetCollectionByID returns collection info from etcd with provided id.
func GetCollectionByID(cli *clientv3.Client, basePath string, collID int64) (*etcdpb.CollectionInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	resp, err := cli.Get(ctx, path.Join(basePath, "root-coord/collection", strconv.FormatInt(collID, 10)))

	if err != nil {
		return nil, err
	}

	if len(resp.Kvs) != 1 {
		return nil, errors.New("invalid collection id")
	}

	if bytes.Equal(resp.Kvs[0].Value, CollectionTombstone) {
		return nil, fmt.Errorf("%w, collection id: %d", ErrCollectionDropped, collID)
	}

	coll := &etcdpb.CollectionInfo{}

	err = proto.Unmarshal(resp.Kvs[0].Value, coll)
	if err != nil {
		return nil, err
	}

	err = FillFieldSchemaIfEmpty(cli, basePath, coll)
	if err != nil {
		return nil, err
	}

	return coll, nil
}

func FillFieldSchemaIfEmpty(cli *clientv3.Client, basePath string, collection *etcdpb.CollectionInfo) error {
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

// ListLoadedCollectionInfoV2_1 returns collection info from querycoord milvus v2.1.x.
func ListLoadedCollectionInfoV2_1(cli *clientv3.Client, basePath string) ([]*querypb.CollectionInfo, error) {
	prefix := path.Join(basePath, CollectionLoadPrefix)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	resp, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	ret := make([]*querypb.CollectionInfo, 0)
	for _, kv := range resp.Kvs {
		collectionInfo := &querypb.CollectionInfo{}
		err = proto.Unmarshal(kv.Value, collectionInfo)
		if err != nil {
			return nil, err
		}
		ret = append(ret, collectionInfo)
	}
	return ret, nil
}

// ListLoadedCollectionInfoV2_1 returns collection info from querycoord milvus v2.2.x.
func ListLoadedCollectionInfoV2_2(cli *clientv3.Client, basePath string) ([]querypbv2.CollectionLoadInfo, error) {
	prefix := path.Join(basePath, CollectionLoadPrefixV2)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	info, _, err := ListProtoObjects[querypbv2.CollectionLoadInfo](ctx, cli, prefix)
	return info, err
}
