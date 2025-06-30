package binlogv2

import "github.com/milvus-io/birdwatcher/storage/common"

type BinlogReader struct {
	common.ReadSeeker
}
