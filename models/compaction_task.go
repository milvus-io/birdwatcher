package models

import "github.com/milvus-io/milvus/pkg/v2/proto/datapb"

// CompactionTask model for collection compaction task information.
type CompactionTask struct {
	*datapb.CompactionTask
	// etcd collection key
	key string
}

func (ct *CompactionTask) Key() string {
	return ct.key
}

// NewCompactionTask parses compactionTask(proto v2.2) to models.CompactionTask
func NewCompactionTask(info *datapb.CompactionTask, key string) *CompactionTask {
	ct := &CompactionTask{
		CompactionTask: info,
		key:            key,
	}

	return ct
}
