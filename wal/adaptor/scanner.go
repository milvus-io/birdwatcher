package adaptor

import (
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls"
)

type Scanner interface {
	Channel() types.PChannelInfo
	Chan() <-chan message.ImmutableMessage
	Close() error
}

func NewScanner(l walimpls.ROWALImpls,
	readOption ReadOption,
) Scanner {
	scannerName := l.Channel().Name
	cleanup := func() {
		l.Close()
	}
	return newScannerAdaptor(scannerName, l, readOption, cleanup)
}
