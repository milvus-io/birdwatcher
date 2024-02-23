package storage

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/milvus-io/birdwatcher/proto/v2.0/schemapb"
)

const (
	// MagicNumber used in binlog
	MagicNumber int32 = 0xfffabc
)

// BinlogReader from Milvus.
type BinlogReader struct {
}

type descriptorEvent struct {
	descriptorEventHeader
	descriptorEventData
}

func NewBinlogReader(f io.Reader) (*BinlogReader, descriptorEvent, error) {
	reader := &BinlogReader{}
	var de descriptorEvent
	var err error

	if _, err = reader.readMagicNumber(f); err != nil {
		return nil, de, err
	}

	if de, err = reader.readDescriptorEvent(f); err != nil {
		return nil, de, err
	}

	return reader, de, nil
}

// NextInt64EventReader returns next reader for the events.
func (reader *BinlogReader) NextInt64EventReader(f io.Reader) ([]int64, error) {
	eventReader := newEventReader()
	header, err := eventReader.readHeader(f)
	if err != nil {
		return nil, err
	}
	insertEventData, err := readInsertEventData(f)
	if err != nil {
		return nil, err
	}

	next := int(header.EventLength - header.GetMemoryUsageInBytes() - insertEventData.GetEventDataFixPartSize())

	data := make([]byte, next)
	io.ReadFull(f, data)

	pr, err := NewParquetPayloadReader(schemapb.DataType_Int64, data)
	if err != nil {
		return nil, err
	}

	return pr.GetInt64sFromPayload()
}

func (reader *BinlogReader) NextVarcharEventReader(f io.Reader) ([]string, error) {
	eventReader := newEventReader()
	header, err := eventReader.readHeader(f)
	if err != nil {
		return nil, err
	}
	insertEventData, err := readInsertEventData(f)
	if err != nil {
		return nil, err
	}

	next := int(header.EventLength - header.GetMemoryUsageInBytes() - insertEventData.GetEventDataFixPartSize())

	data := make([]byte, next)
	io.ReadFull(f, data)

	pr, err := NewParquetPayloadReader(schemapb.DataType_Int64, data)
	if err != nil {
		return nil, err
	}

	return pr.GetStringFromPayload()

}

func (reader *BinlogReader) readMagicNumber(f io.Reader) (int32, error) {
	var err error
	var magicNumber int32
	magicNumber, err = readMagicNumber(f)

	return magicNumber, err
}

func (reader *BinlogReader) readDescriptorEvent(f io.Reader) (descriptorEvent, error) {
	event, err := ReadDescriptorEvent(f)
	if err != nil {
		return event, err
	}
	return event, nil
}

func readMagicNumber(buffer io.Reader) (int32, error) {
	var magicNumber int32
	if err := binary.Read(buffer, commonEndian, &magicNumber); err != nil {
		return -1, err
	}
	if magicNumber != MagicNumber {
		return -1, fmt.Errorf("parse magic number failed, expected: %d, actual: %d", MagicNumber, magicNumber)
	}

	return magicNumber, nil
}

// ReadDescriptorEvent reads a descriptorEvent from buffer
func ReadDescriptorEvent(buffer io.Reader) (descriptorEvent, error) {
	de := descriptorEvent{}
	header, err := readDescriptorEventHeader(buffer)
	if err != nil {
		return de, err
	}
	data, err := readDescriptorEventData(buffer)
	if err != nil {
		return de, err
	}
	return descriptorEvent{
		descriptorEventHeader: *header,
		descriptorEventData:   *data,
	}, nil
}

// Close closes the BinlogReader object.
// It mainly calls the Close method of the internal events, reclaims resources, and marks itself as closed.
func (reader *BinlogReader) Close() {
}
