package common

import (
	"bytes"
	"context"
	"path"
	"strconv"
	"strings"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/proto/v2.0/etcdpb"
	etcdpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/etcdpb"
	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/samber/lo"
	"google.golang.org/protobuf/runtime/protoiface"
)

func ListCollectionHistoryWithDB(ctx context.Context, cli kv.MetaKV, basePath string, version string, dbID, collectionID int64) ([]*models.CollectionHistory, error) {
	var prefix string

	if dbID > 0 {
		prefix = path.Join(basePath, "snapshots/root-coord/database/collection-info", strconv.FormatInt(dbID, 10), strconv.FormatInt(collectionID, 10))
	} else {
		prefix = path.Join(basePath, "snapshots/root-coord/collection", strconv.FormatInt(collectionID, 10))
	}

	var dropped, paths []string
	var err error
	var result []*models.CollectionHistory
	switch version {
	case models.LTEVersion2_1:
		var colls []etcdpb.CollectionInfo
		colls, paths, dropped, err = ListHistoryCollection[etcdpb.CollectionInfo](ctx, cli, prefix)
		if err != nil {
			return nil, err
		}
		result = lo.Map(colls, func(coll etcdpb.CollectionInfo, idx int) *models.CollectionHistory {
			ch := &models.CollectionHistory{}
			ch.Collection = *models.NewCollectionFromV2_1(&coll, paths[idx])
			ch.Ts = parseHistoryTs(paths[idx])
			return ch
		})
	case models.GTEVersion2_2:
		var colls []etcdpbv2.CollectionInfo
		colls, paths, dropped, err = ListHistoryCollection[etcdpbv2.CollectionInfo](ctx, cli, prefix)
		if err != nil {
			return nil, err
		}
		result = lo.Map(colls, func(coll etcdpbv2.CollectionInfo, idx int) *models.CollectionHistory {
			ch := &models.CollectionHistory{}
			//TODO add history field schema
			ch.Collection = *models.NewCollectionFromV2_2(&coll, paths[idx], nil)
			ch.Ts = parseHistoryTs(paths[idx])
			return ch
		})
	}

	for _, entry := range dropped {
		collHistory := &models.CollectionHistory{Dropped: true}
		collHistory.Ts = parseHistoryTs(entry)
		result = append(result, collHistory)
	}

	return result, nil
}

// ListCollectionHistory list collection history from snapshots.
func ListCollectionHistory(ctx context.Context, cli kv.MetaKV, basePath string, version string, dbID, collectionID int64) ([]*models.CollectionHistory, error) {
	var prefix string
	if dbID == 0 {
		prefix = path.Join(basePath, "snapshots/root-coord/collection", strconv.FormatInt(collectionID, 10))
	} else {
		prefix = path.Join(basePath, "snapshots/root-coord/database/collection-info", strconv.FormatInt(dbID, 10), strconv.FormatInt(collectionID, 10))
	}

	var dropped, paths []string
	var err error
	var result []*models.CollectionHistory
	switch version {
	case models.LTEVersion2_1:
		var colls []etcdpb.CollectionInfo
		colls, paths, dropped, err = ListHistoryCollection[etcdpb.CollectionInfo](ctx, cli, prefix)
		if err != nil {
			return nil, err
		}
		result = lo.Map(colls, func(coll etcdpb.CollectionInfo, idx int) *models.CollectionHistory {
			ch := &models.CollectionHistory{}
			ch.Collection = *models.NewCollectionFromV2_1(&coll, paths[idx])
			ch.Ts = parseHistoryTs(paths[idx])
			return ch
		})
	case models.GTEVersion2_2:
		var colls []etcdpbv2.CollectionInfo
		colls, paths, dropped, err = ListHistoryCollection[etcdpbv2.CollectionInfo](ctx, cli, prefix)
		if err != nil {
			return nil, err
		}
		result = lo.Map(colls, func(coll etcdpbv2.CollectionInfo, idx int) *models.CollectionHistory {
			ch := &models.CollectionHistory{}
			//TODO add history field schema
			ch.Collection = *models.NewCollectionFromV2_2(&coll, paths[idx], nil)
			ch.Ts = parseHistoryTs(paths[idx])
			return ch
		})
	}

	for _, entry := range dropped {
		collHistory := &models.CollectionHistory{Dropped: true}
		collHistory.Ts = parseHistoryTs(entry)
		result = append(result, collHistory)
	}

	return result, nil
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
	protoiface.MessageV1
}](ctx context.Context, cli kv.MetaKV, prefix string) ([]T, []string, []string, error) {
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
	colls, paths, _, err := ListHistoryCollection[etcdpbv2.CollectionInfo](ctx, cli, prefix)
	if err != nil {
		return err
	}
	for idx, coll := range colls {
		if coll.GetID() != collectionID {
			continue
		}
		if coll.State == etcdpbv2.CollectionState_CollectionDropped || coll.State == etcdpbv2.CollectionState_CollectionDropping {
			cli.Remove(ctx, paths[idx])
		}
	}
	return nil
}
