package common

import (
	"context"
	"path"
	"strconv"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
)

const (
	walBroadcastPrefix = "streamingcoord-meta/broadcast-task/"
)

var ErrBroadcastTaskNotFound = errors.New("broadcast task not found")

// ListWalBroadcast list broadcast tasks.
func ListWalBroadcast(ctx context.Context, cli kv.MetaKV, basePath string) ([]*streamingpb.BroadcastTask, error) {
	prefix := path.Join(basePath, walBroadcastPrefix) + "/"
	metas, _, err := ListProtoObjects[streamingpb.BroadcastTask](ctx, cli, prefix)
	if err != nil {
		return nil, err
	}
	return metas, nil
}

// ListWalBroadcastByID list broadcast task by broadcast ID.
func ListWalBroadcastByID(ctx context.Context, cli kv.MetaKV, basePath string, broadcastID int64) (*streamingpb.BroadcastTask, error) {
	prefix := path.Join(basePath, walBroadcastPrefix, strconv.FormatInt(broadcastID, 10))
	metas, _, err := ListProtoObjects[streamingpb.BroadcastTask](ctx, cli, prefix)
	if err != nil {
		return nil, err
	}
	if len(metas) == 0 {
		return nil, ErrBroadcastTaskNotFound
	}
	return metas[0], nil
}

// SaveWalBroadcastTask reset wal broadcast task.
func SaveWalBroadcastTask(ctx context.Context, cli kv.MetaKV, basePath string, broadcastID int64, broadcastTask *streamingpb.BroadcastTask) error {
	prefix := path.Join(basePath, walBroadcastPrefix, strconv.FormatInt(broadcastID, 10))
	bs, err := proto.Marshal(broadcastTask)
	if err != nil {
		return err
	}
	return cli.Save(ctx, prefix, string(bs))
}

// RemoveWalBroadcastTask remove wal broadcast task.
func RemoveWalBroadcastTask(ctx context.Context, cli kv.MetaKV, basePath string, broadcastID int64) error {
	prefix := path.Join(basePath, walBroadcastPrefix, strconv.FormatInt(broadcastID, 10))
	return cli.RemoveWithPrefix(ctx, prefix)
}
