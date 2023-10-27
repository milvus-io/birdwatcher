package common

import (
	"context"
	"path"

	"github.com/milvus-io/birdwatcher/states/kv"
)

func ListEtcdConfigs(ctx context.Context, cli kv.MetaKV, basePath string) (keys, values []string, err error) {
	prefix := path.Join(basePath, "config")
	return cli.LoadWithPrefix(ctx, prefix)
}

func SetEtcdConfig(ctx context.Context, cli kv.MetaKV, basePath string, key, value string) error {
	prefix := path.Join(basePath, "config", key)
	return cli.Save(ctx, prefix, value)
}

func RemoveEtcdConfig(ctx context.Context, cli kv.MetaKV, basePath string, key string) error {
	prefix := path.Join(basePath, "config", key)
	return cli.Remove(ctx, prefix)
}
