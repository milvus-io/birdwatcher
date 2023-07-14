package common

import (
	"context"
	"path"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func ListEtcdConfigs(ctx context.Context, cli clientv3.KV, basePath string) (keys, values []string, err error) {
	prefix := path.Join(basePath, "config")
	return listEtcdConfigsByPrefix(ctx, cli, prefix)
}

func listEtcdConfigsByPrefix(ctx context.Context, cli clientv3.KV, prefix string) (keys, values []string, err error) {
	resp, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, nil, err
	}

	keys = make([]string, 0, len(resp.Kvs))
	values = make([]string, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		keys = append(keys, string(kv.Key))
		values = append(values, string(kv.Value))
	}
	return keys, values, nil
}

func SetEtcdConfig(ctx context.Context, cli clientv3.KV, basePath string, key, value string) error {
	prefix := path.Join(basePath, "config")
	_, err := cli.Put(ctx, path.Join(prefix, key), value)
	return err
}

func RemoveEtcdConfig(ctx context.Context, cli clientv3.KV, basePath string, key string) error {
	prefix := path.Join(basePath, "config")
	_, err := cli.Delete(ctx, path.Join(prefix, key))
	return err
}
