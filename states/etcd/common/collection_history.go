package common

import (
	"bytes"
	"context"
	"path"
	"strconv"
	"strings"

	"github.com/samber/lo"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
)

func ListCollectionHistory(ctx context.Context, cli kv.MetaKV, basePath string, dbID, collectionID int64) ([]*models.CollectionHistory, error) {
	var prefix string
	if dbID == 0 {
		prefix = path.Join(basePath, "snapshots/root-coord/collection", strconv.FormatInt(collectionID, 10))
	} else {
		prefix = path.Join(basePath, "snapshots/root-coord/database/collection-info", strconv.FormatInt(dbID, 10), strconv.FormatInt(collectionID, 10))
	}

	var results []*models.CollectionHistory

	colls, paths, dropped, err := ListHistoryCollection[etcdpb.CollectionInfo](ctx, cli, prefix)
	if err != nil {
		return nil, err
	}
	results = lo.Map(colls, func(coll *etcdpb.CollectionInfo, idx int) *models.CollectionHistory {
		ch := &models.CollectionHistory{}
		// TODO add history field schema
		ch.Collection = models.NewCollection(coll, paths[idx])
		ch.Ts = parseHistoryTs(paths[idx])
		return ch
	})

	for _, entry := range dropped {
		collHistory := &models.CollectionHistory{Dropped: true}
		collHistory.Ts = parseHistoryTs(entry)
		results = append(results, collHistory)
	}

	return results, nil
}

func parseHistoryTs(entry string) uint64 {
	parts := strings.Split(entry, "_ts")
	if len(parts) != 2 {
		return 0
	}

	result, _ := strconv.ParseUint(parts[1], 10, 64)
	return result
}

func ListHistoryCollection[T any, P interface {
	*T
	proto.Message
}](ctx context.Context, cli kv.MetaKV, prefix string) ([]*T, []string, []string, error) {
	var dropped []string
	colls, paths, err := ListProtoObjectsAdv[T, P](ctx, cli, prefix,
		func(key string, value []byte) bool {
			isTombstone := bytes.Equal(value, CollectionTombstone)
			if isTombstone {
				dropped = append(dropped, key)
			}
			return !isTombstone
		})
	if err != nil {
		return nil, nil, nil, err
	}

	return colls, paths, dropped, nil
}

func RemoveCollectionHistory(ctx context.Context, cli kv.MetaKV, basePath string, version string, collectionID int64) error {
	prefix := path.Join(basePath, "snapshots/root-coord/collection", strconv.FormatInt(collectionID, 10))
	colls, paths, _, err := ListHistoryCollection[etcdpb.CollectionInfo](ctx, cli, prefix)
	if err != nil {
		return err
	}
	for idx, coll := range colls {
		if coll.GetID() != collectionID {
			continue
		}
		if coll.State == etcdpb.CollectionState_CollectionDropped || coll.State == etcdpb.CollectionState_CollectionDropping {
			cli.Remove(ctx, paths[idx])
		}
	}
	return nil
}
