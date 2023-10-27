package kv

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"os"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/birdwatcher/models"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

// implementation assertion
var _ MetaKV = (*FileAuditKV)(nil)

type FileAuditKV struct {
	cli  MetaKV
	file *os.File
}

// NewFileAuditKV creates a file auditing log kv.
func NewFileAuditKV(kv MetaKV, file *os.File) *FileAuditKV {
	return &FileAuditKV{
		cli:  kv,
		file: file,
	}
}

func (c *FileAuditKV) Load(ctx context.Context, key string) (string, error) {
	return c.cli.Load(ctx, key)
}

func (c *FileAuditKV) LoadWithPrefix(ctx context.Context, key string) ([]string, []string, error) {
	return c.cli.LoadWithPrefix(ctx, key)
}

func (c *FileAuditKV) Save(ctx context.Context, key, value string) error {
	c.writeHeader(models.OpPut, 2)
	err := c.cli.Save(ctx, key, value)
	if err == nil {
		c.writeHeader(models.OpPutBefore, 1)
		c.writeKeyValue(key, value)
	}
	c.writeHeader(models.OpPutAfter, 1)
	return err
}

func (c *FileAuditKV) Remove(ctx context.Context, key string) error {
	fmt.Println("audit delete", key)
	kv, err := c.cli.removeWithPrevKV(ctx, key)
	if err != nil {
		return err
	}
	c.writeHeader(models.OpDel, 1)
	c.writeLogKV(kv)
	return nil
}

func (c *FileAuditKV) RemoveWithPrefix(ctx context.Context, key string) error {
	fmt.Println("audit delete with prefix", key)
	kvs, err := c.cli.removeWithPrefixAndPrevKV(ctx, key)
	if err != nil {
		return err
	}
	c.writeHeader(models.OpDel, int32(len(kvs)))
	for _, kv := range kvs {
		c.writeLogKV(kv)
	}
	return nil
}

func (c *FileAuditKV) removeWithPrevKV(ctx context.Context, key string) (*mvccpb.KeyValue, error) {
	return c.cli.removeWithPrevKV(ctx, key)
}

func (c *FileAuditKV) removeWithPrefixAndPrevKV(ctx context.Context, prefix string) ([]*mvccpb.KeyValue, error) {
	return c.cli.removeWithPrefixAndPrevKV(ctx, prefix)
}

func (c *FileAuditKV) GetAllRootPath(ctx context.Context) ([]string, error) {
	return c.cli.GetAllRootPath(ctx)
}

func (c *FileAuditKV) Close() {
	c.cli.Close()
}

func (c *FileAuditKV) BackupKV(base, prefix string, w *bufio.Writer, ignoreRevision bool, batchSize int64) error {
	return c.cli.BackupKV(base, prefix, w, ignoreRevision, batchSize)
}

func (c *FileAuditKV) writeHeader(op models.AuditOpType, entriesNum int32) {
	header := &models.AuditHeader{
		Version:    1,
		OpType:     int32(op),
		EntriesNum: entriesNum,
	}
	bs, _ := proto.Marshal(header)
	c.writeData(bs)
}

func (c *FileAuditKV) writeLogKV(kv *mvccpb.KeyValue) {
	bs, _ := proto.Marshal(kv)
	c.writeData(bs)
}

func (c *FileAuditKV) writeKeyValue(key, value string) {
	kv := &mvccpb.KeyValue{
		Key:   []byte(key),
		Value: []byte(value),
	}
	c.writeLogKV(kv)
}

func (c *FileAuditKV) writeData(data []byte) {
	lb := make([]byte, 8)
	binary.LittleEndian.PutUint64(lb, uint64(len(data)))
	_, err := c.file.Write(lb)
	if err != nil {
		fmt.Println("failed to write audit header", err.Error())
		return
	}
	if len(data) > 0 {
		_, err = c.file.Write(data)
		if err != nil {
			fmt.Println("failed to write audit log", err.Error())
		}
	}
}
