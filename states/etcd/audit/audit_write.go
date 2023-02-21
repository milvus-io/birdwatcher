package audit

import (
	"encoding/binary"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/birdwatcher/models"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

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
	n, err := c.file.Write(lb)
	fmt.Println(n, err)
	if len(data) > 0 {
		n, err = c.file.Write(data)
		fmt.Println(n, err)
	}
}
