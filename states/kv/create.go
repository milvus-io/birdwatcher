package kv

import (
	"context"

	"github.com/cockroachdb/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// CreateEtcdKeyIfAbsent atomically creates a metadata key without overwriting it.
// A read followed by Save is insufficient: another writer may create the key in between.
func CreateEtcdKeyIfAbsent(ctx context.Context, cli MetaKV, key, value string) error {
	etcd, ok := cli.(*etcdKV)
	if !ok {
		return errors.New("create-if-absent repair requires a live etcd connection")
	}
	key = joinPath(etcd.rootPath, key)
	resp, err := etcd.client.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(key), "=", 0)).
		Then(clientv3.OpPut(key, value)).Commit()
	if err != nil {
		return errors.Wrap(err, "create import marker transaction failed; inspect the key before retrying")
	}
	if !resp.Succeeded {
		return errors.Newf("key %s already exists; refusing to overwrite", key)
	}
	return nil
}
