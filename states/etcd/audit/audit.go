package audit

import (
	"context"
	"fmt"
	"os"

	"github.com/milvus-io/birdwatcher/models"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type FileAuditKV struct {
	cli  clientv3.KV
	file *os.File
}

// NewFileAuditKV creates a file auditing log kv.
func NewFileAuditKV(kv clientv3.KV, file *os.File) *FileAuditKV {
	return &FileAuditKV{
		cli:  kv,
		file: file,
	}
}

// Put puts a key-value pair into etcd.
// Note that key,value can be plain bytes array and string is
// an immutable representation of that bytes array.
// To get a string of bytes, do string([]byte{0x10, 0x20}).
func (c *FileAuditKV) Put(ctx context.Context, key string, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	opts = append(opts, clientv3.WithPrevKV())
	resp, err := c.cli.Put(ctx, key, val, opts...)
	c.writeHeader(models.OpPut, 2)
	if resp.PrevKv != nil {
		c.writeHeader(models.OpPutBefore, 1)
		c.writeLogKV(resp.PrevKv)
	}
	c.writeHeader(models.OpPutAfter, 1)

	return resp, err
}

// Get retrieves keys.
// By default, Get will return the value for "key", if any.
// When passed WithRange(end), Get will return the keys in the range [key, end).
// When passed WithFromKey(), Get returns keys greater than or equal to key.
// When passed WithRev(rev) with rev > 0, Get retrieves keys at the given revision;
// if the required revision is compacted, the request will fail with ErrCompacted .
// When passed WithLimit(limit), the number of returned keys is bounded by limit.
// When passed WithSort(), the keys will be sorted.
func (c *FileAuditKV) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	return c.cli.Get(ctx, key, opts...)
}

// Delete deletes a key, or optionally using WithRange(end), [key, end).
func (c *FileAuditKV) Delete(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	fmt.Println("audit delete", key)
	opts = append(opts, clientv3.WithPrevKV())
	resp, err := c.cli.Delete(ctx, key, opts...)
	if err != nil {
		return resp, err
	}
	c.writeHeader(models.OpDel, int32(len(resp.PrevKvs)))
	for _, kv := range resp.PrevKvs {
		c.writeLogKV(kv)
	}
	return resp, err
}

// Compact compacts etcd KV history before the given rev.
func (c *FileAuditKV) Compact(ctx context.Context, rev int64, opts ...clientv3.CompactOption) (*clientv3.CompactResponse, error) {
	return c.cli.Compact(ctx, rev, opts...)
}

// Do applies a single Op on KV without a transaction.
// Do is useful when creating arbitrary operations to be issued at a
// later time; the user can range over the operations, calling Do to
// execute them. Get/Put/Delete, on the other hand, are best suited
// for when the operation should be issued at the time of declaration.
func (c *FileAuditKV) Do(ctx context.Context, op clientv3.Op) (clientv3.OpResponse, error) {
	resp, err := c.cli.Do(ctx, op)
	// TODO add do audit
	return resp, err
}

// Txn creates a transaction.
func (c *FileAuditKV) Txn(ctx context.Context) clientv3.Txn {
	return c.cli.Txn(ctx)
}
