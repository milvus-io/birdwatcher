package common

import (
	"bytes"
	"context"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/milvus-io/birdwatcher/proto/v2.0/etcdpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type CollectionHistory struct {
	Info    etcdpb.CollectionInfo
	Ts      uint64
	Dropped bool
}

// ListCollectionHistory list collection history from snapshots.
func ListCollectionHistory(cli *clientv3.Client, basePath string, collectionID int64) ([]CollectionHistory, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	var dropped []string
	colls, paths, err := ListProtoObjectsAdv[etcdpb.CollectionInfo](ctx, cli, path.Join(basePath, "snapshots/root-coord/collection", strconv.FormatInt(collectionID, 10)),
		func(key string, value []byte) bool {
			isTombstone := bytes.Equal(value, CollectionTombstone)
			if isTombstone {
				dropped = append(dropped, key)
			}
			return !isTombstone
		})
	if err != nil {
		return nil, err
	}

	result := make([]CollectionHistory, 0, len(colls)+len(dropped))
	for _, entry := range dropped {
		collHistory := CollectionHistory{Dropped: true}
		parts := strings.Split(entry, "_ts")
		if len(parts) == 2 {
			collHistory.Ts, _ = strconv.ParseUint(parts[1], 10, 64)
		}
		result = append(result, collHistory)
	}

	for idx, coll := range colls {
		collHistory := CollectionHistory{
			Info: coll,
		}
		path := paths[idx]
		parts := strings.Split(path, "_ts")
		if len(parts) == 2 {
			collHistory.Ts, _ = strconv.ParseUint(parts[1], 10, 64)
		}
		result = append(result, collHistory)
	}
	return result, nil
}
