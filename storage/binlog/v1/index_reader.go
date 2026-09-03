package binlogv1

import (
	"errors"
	"fmt"
	"io"

	"github.com/samber/lo"

	storagecommon "github.com/milvus-io/birdwatcher/storage/common"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

type IndexReader struct{}

func NewIndexReader(f storagecommon.ReadSeeker) (*IndexReader, DescriptorEvent, error) {
	reader := &IndexReader{}
	var de DescriptorEvent
	var err error

	_, err = ReadMagicNumber(f)
	if err != nil {
		return nil, de, err
	}

	de, err = ReadDescriptorEvent(f)
	if err != nil {
		return nil, de, err
	}
	return reader, de, err
}

func (reader *IndexReader) NextEventReader(f io.Reader, dataType schemapb.DataType) ([][]byte, error) {
	data, err := readIndexEventPayload(f)
	if err != nil {
		return nil, err
	}

	pr, err := NewParquetPayloadReader(dataType, data)
	if err != nil {
		return nil, err
	}
	switch dataType {
	case schemapb.DataType_String:
		result, err := pr.GetStringFromPayload(0)
		if err != nil {
			return nil, err
		}
		return lo.Map(result, func(data string, _ int) []byte {
			return []byte(data)
		}), nil
	case schemapb.DataType_Int8:
		result, err := pr.GetBytesFromPayload(0)
		if err != nil {
			return nil, err
		}
		return [][]byte{result}, nil
	}
	return nil, errors.New("unexpected data type")
}

// NextRawEventReader returns a non-encoded index payload. Milvus stores index
// blobs directly in the event payload when the descriptor data type is None.
func (reader *IndexReader) NextRawEventReader(f io.Reader) ([]byte, error) {
	return readIndexEventPayload(f)
}

func readIndexEventPayload(f io.Reader) ([]byte, error) {
	eventReader := NewEventReader()
	header, err := eventReader.ReadHeader(f)
	if err != nil {
		return nil, err
	}
	if header.TypeCode != IndexFileEventType {
		return nil, fmt.Errorf("unexpected index event type %d", header.TypeCode)
	}
	ifed, err := ReadIndexFileEventData(f)
	if err != nil {
		return nil, err
	}

	next := header.EventLength - header.GetMemoryUsageInBytes() - ifed.GetEventDataFixPartSize()
	if next < 0 {
		return nil, fmt.Errorf("invalid index event length %d", header.EventLength)
	}
	data := make([]byte, next)
	if _, err := io.ReadFull(f, data); err != nil {
		return nil, err
	}
	return data, nil
}
