package binlogv1

import (
	"encoding/binary"
	"io"
)

// EventTypeCode represents event type by code
type EventTypeCode int8

// EventTypeCode definitions
const (
	DescriptorEventType EventTypeCode = iota
	InsertEventType
	DeleteEventType
	CreateCollectionEventType
	DropCollectionEventType
	CreatePartitionEventType
	DropPartitionEventType
	IndexFileEventType
	EventTypeEnd
)

var commonEndian = binary.LittleEndian

type baseEventHeader struct {
	Timestamp uint64
	TypeCode  EventTypeCode

	EventLength  int32
	NextPosition int32
}

func (header *baseEventHeader) GetMemoryUsageInBytes() int32 {
	return int32(binary.Size(header))
}

func (header *baseEventHeader) Write(buffer io.Writer) error {
	return binary.Write(buffer, commonEndian, header)
}

type descriptorEventHeader = baseEventHeader

type eventHeader struct {
	baseEventHeader
}

func readEventHeader(buffer io.Reader) (*eventHeader, error) {
	header := &eventHeader{}
	if err := binary.Read(buffer, commonEndian, header); err != nil {
		return nil, err
	}

	return header, nil
}

func readDescriptorEventHeader(buffer io.Reader) (*descriptorEventHeader, error) {
	header := &descriptorEventHeader{}
	if err := binary.Read(buffer, commonEndian, header); err != nil {
		return nil, err
	}
	return header, nil
}

func newDescriptorEventHeader() *descriptorEventHeader {
	header := descriptorEventHeader{
		Timestamp: 0, // tsoutil.ComposeTS(time.Now().UnixNano()/int64(time.Millisecond), 0),
		TypeCode:  DescriptorEventType,
	}
	return &header
}

func newEventHeader(eventTypeCode EventTypeCode) *eventHeader {
	return &eventHeader{
		baseEventHeader: baseEventHeader{
			Timestamp:    0, // tsoutil.ComposeTS(time.Now().UnixNano()/int64(time.Millisecond), 0),
			TypeCode:     eventTypeCode,
			EventLength:  -1,
			NextPosition: -1,
		},
	}
}
