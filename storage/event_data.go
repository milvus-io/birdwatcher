package storage

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"

	"github.com/milvus-io/birdwatcher/proto/v2.0/schemapb"
)

const originalSizeKey = "original_size"

type descriptorEventData struct {
	DescriptorEventDataFixPart
	ExtraLength       int32
	ExtraBytes        []byte
	Extras            map[string]interface{}
	PostHeaderLengths []uint8
}

// DescriptorEventDataFixPart is a memory struct saves events' DescriptorEventData.
type DescriptorEventDataFixPart struct {
	CollectionID    int64
	PartitionID     int64
	SegmentID       int64
	FieldID         int64
	StartTimestamp  uint64
	EndTimestamp    uint64
	PayloadDataType schemapb.DataType
}

// SetEventTimeStamp set the timestamp value of DescriptorEventDataFixPart.
func (data *descriptorEventData) SetEventTimeStamp(start uint64, end uint64) {
	data.StartTimestamp = start
	data.EndTimestamp = end
}

// GetEventDataFixPartSize returns the memory size of DescriptorEventDataFixPart.
func (data *descriptorEventData) GetEventDataFixPartSize() int32 {
	return int32(binary.Size(data.DescriptorEventDataFixPart))
}

// GetMemoryUsageInBytes returns the memory size of DescriptorEventDataFixPart.
func (data *descriptorEventData) GetMemoryUsageInBytes() int32 {
	return data.GetEventDataFixPartSize() + int32(binary.Size(data.PostHeaderLengths)) + int32(binary.Size(data.ExtraLength)) + data.ExtraLength
}

// AddExtra add extra params to description event.
func (data *descriptorEventData) AddExtra(k string, v interface{}) {
	data.Extras[k] = v
}

// FinishExtra marshal extras to json format.
// Call before GetMemoryUsageInBytes to get an accurate length of description event.
func (data *descriptorEventData) FinishExtra() error {
	var err error

	// keep all binlog file records the original size
	sizeStored, ok := data.Extras[originalSizeKey]
	if !ok {
		return fmt.Errorf("%v not in extra", originalSizeKey)
	}
	// if we store a large int directly, golang will use scientific notation, we then will get a float value.
	// so it's better to store the original size in string format.
	sizeStr, ok := sizeStored.(string)
	if !ok {
		return fmt.Errorf("value of %v must in string format", originalSizeKey)
	}
	_, err = strconv.Atoi(sizeStr)
	if err != nil {
		return fmt.Errorf("value of %v must be able to be converted into int format", originalSizeKey)
	}

	data.ExtraBytes, err = json.Marshal(data.Extras)
	if err != nil {
		return err
	}
	data.ExtraLength = int32(len(data.ExtraBytes))
	return nil
}

// Write transfer DescriptorEventDataFixPart to binary buffer.
func (data *descriptorEventData) Write(buffer io.Writer) error {
	if err := binary.Write(buffer, commonEndian, data.DescriptorEventDataFixPart); err != nil {
		return err
	}
	if err := binary.Write(buffer, commonEndian, data.PostHeaderLengths); err != nil {
		return err
	}
	if err := binary.Write(buffer, commonEndian, data.ExtraLength); err != nil {
		return err
	}
	return binary.Write(buffer, commonEndian, data.ExtraBytes)
}

func readDescriptorEventData(buffer io.Reader) (*descriptorEventData, error) {
	event := newDescriptorEventData()
	if err := binary.Read(buffer, commonEndian, &event.DescriptorEventDataFixPart); err != nil {
		return nil, err
	}
	if err := binary.Read(buffer, commonEndian, &event.PostHeaderLengths); err != nil {
		return nil, err
	}

	if err := binary.Read(buffer, commonEndian, &event.ExtraLength); err != nil {
		return nil, err
	}
	event.ExtraBytes = make([]byte, event.ExtraLength)
	if err := binary.Read(buffer, commonEndian, &event.ExtraBytes); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(event.ExtraBytes, &event.Extras); err != nil {
		return nil, err
	}

	return event, nil
}

type eventData interface {
	GetEventDataFixPartSize() int32
	WriteEventData(buffer io.Writer) error
}

// all event types' fixed part only have start Timestamp and end Timestamp yet, but maybe different events will
// have different fields later, so we just create an event data struct per event type.
type insertEventData struct {
	StartTimestamp uint64
	EndTimestamp   uint64
}

func (data *insertEventData) SetEventTimestamp(start uint64, end uint64) {
	data.StartTimestamp = start
	data.EndTimestamp = end
}

func (data *insertEventData) GetEventDataFixPartSize() int32 {
	return int32(binary.Size(data))
}

func (data *insertEventData) WriteEventData(buffer io.Writer) error {
	if data.StartTimestamp == 0 {
		return errors.New("hasn't set start time stamp")
	}
	if data.EndTimestamp == 0 {
		return errors.New("hasn't set end time stamp")
	}
	return binary.Write(buffer, commonEndian, data)
}

type deleteEventData struct {
	StartTimestamp uint64
	EndTimestamp   uint64
}

func (data *deleteEventData) SetEventTimestamp(start uint64, end uint64) {
	data.StartTimestamp = start
	data.EndTimestamp = end
}

func (data *deleteEventData) GetEventDataFixPartSize() int32 {
	return int32(binary.Size(data))
}

func (data *deleteEventData) WriteEventData(buffer io.Writer) error {
	if data.StartTimestamp == 0 {
		return errors.New("hasn't set start time stamp")
	}
	if data.EndTimestamp == 0 {
		return errors.New("hasn't set end time stamp")
	}
	return binary.Write(buffer, commonEndian, data)
}

type createCollectionEventData struct {
	StartTimestamp uint64
	EndTimestamp   uint64
}

func (data *createCollectionEventData) SetEventTimestamp(start uint64, end uint64) {
	data.StartTimestamp = start
	data.EndTimestamp = end
}

func (data *createCollectionEventData) GetEventDataFixPartSize() int32 {
	return int32(binary.Size(data))
}

func (data *createCollectionEventData) WriteEventData(buffer io.Writer) error {
	if data.StartTimestamp == 0 {
		return errors.New("hasn't set start time stamp")
	}
	if data.EndTimestamp == 0 {
		return errors.New("hasn't set end time stamp")
	}
	return binary.Write(buffer, commonEndian, data)
}

type dropCollectionEventData struct {
	StartTimestamp uint64
	EndTimestamp   uint64
}

func (data *dropCollectionEventData) SetEventTimestamp(start uint64, end uint64) {
	data.StartTimestamp = start
	data.EndTimestamp = end
}

func (data *dropCollectionEventData) GetEventDataFixPartSize() int32 {
	return int32(binary.Size(data))
}

func (data *dropCollectionEventData) WriteEventData(buffer io.Writer) error {
	if data.StartTimestamp == 0 {
		return errors.New("hasn't set start time stamp")
	}
	if data.EndTimestamp == 0 {
		return errors.New("hasn't set end time stamp")
	}
	return binary.Write(buffer, commonEndian, data)
}

type createPartitionEventData struct {
	StartTimestamp uint64
	EndTimestamp   uint64
}

func (data *createPartitionEventData) SetEventTimestamp(start uint64, end uint64) {
	data.StartTimestamp = start
	data.EndTimestamp = end
}

func (data *createPartitionEventData) GetEventDataFixPartSize() int32 {
	return int32(binary.Size(data))
}

func (data *createPartitionEventData) WriteEventData(buffer io.Writer) error {
	if data.StartTimestamp == 0 {
		return errors.New("hasn't set start time stamp")
	}
	if data.EndTimestamp == 0 {
		return errors.New("hasn't set end time stamp")
	}
	return binary.Write(buffer, commonEndian, data)
}

type dropPartitionEventData struct {
	StartTimestamp uint64
	EndTimestamp   uint64
}

func (data *dropPartitionEventData) SetEventTimestamp(start, end uint64) {
	data.StartTimestamp = start
	data.EndTimestamp = end
}

func (data *dropPartitionEventData) GetEventDataFixPartSize() int32 {
	return int32(binary.Size(data))
}

func (data *dropPartitionEventData) WriteEventData(buffer io.Writer) error {
	if data.StartTimestamp == 0 {
		return errors.New("hasn't set start time stamp")
	}
	if data.EndTimestamp == 0 {
		return errors.New("hasn't set end time stamp")
	}
	return binary.Write(buffer, commonEndian, data)
}

type indexFileEventData struct {
	StartTimestamp uint64
	EndTimestamp   uint64
}

func (data *indexFileEventData) SetEventTimestamp(start, end uint64) {
	data.StartTimestamp = start
	data.EndTimestamp = end
}

func (data *indexFileEventData) GetEventDataFixPartSize() int32 {
	return int32(binary.Size(data))
}

func (data *indexFileEventData) WriteEventData(buffer io.Writer) error {
	if data.StartTimestamp == 0 {
		return errors.New("hasn't set start time stamp")
	}
	if data.EndTimestamp == 0 {
		return errors.New("hasn't set end time stamp")
	}
	return binary.Write(buffer, commonEndian, data)
}

func getEventFixPartSize(code EventTypeCode) int32 {
	switch code {
	case DescriptorEventType:
		return (&descriptorEventData{}).GetEventDataFixPartSize()
	case InsertEventType:
		return (&insertEventData{}).GetEventDataFixPartSize()
	case DeleteEventType:
		return (&deleteEventData{}).GetEventDataFixPartSize()
	case CreateCollectionEventType:
		return (&createCollectionEventData{}).GetEventDataFixPartSize()
	case DropCollectionEventType:
		return (&dropCollectionEventData{}).GetEventDataFixPartSize()
	case CreatePartitionEventType:
		return (&createPartitionEventData{}).GetEventDataFixPartSize()
	case DropPartitionEventType:
		return (&dropPartitionEventData{}).GetEventDataFixPartSize()
	case IndexFileEventType:
		return (&indexFileEventData{}).GetEventDataFixPartSize()
	default:
		return -1
	}
}

func newDescriptorEventData() *descriptorEventData {
	data := descriptorEventData{
		DescriptorEventDataFixPart: DescriptorEventDataFixPart{
			CollectionID:    -1,
			PartitionID:     -1,
			SegmentID:       -1,
			FieldID:         -1,
			StartTimestamp:  0,
			EndTimestamp:    0,
			PayloadDataType: -1,
		},
		PostHeaderLengths: []uint8{},
		Extras:            make(map[string]interface{}),
	}
	for i := DescriptorEventType; i < EventTypeEnd; i++ {
		size := getEventFixPartSize(i)
		data.PostHeaderLengths = append(data.PostHeaderLengths, uint8(size))
	}
	return &data
}

func newInsertEventData() *insertEventData {
	return &insertEventData{
		StartTimestamp: 0,
		EndTimestamp:   0,
	}
}

func newDeleteEventData() *deleteEventData {
	return &deleteEventData{
		StartTimestamp: 0,
		EndTimestamp:   0,
	}
}

func newCreateCollectionEventData() *createCollectionEventData {
	return &createCollectionEventData{
		StartTimestamp: 0,
		EndTimestamp:   0,
	}
}

func newDropCollectionEventData() *dropCollectionEventData {
	return &dropCollectionEventData{
		StartTimestamp: 0,
		EndTimestamp:   0,
	}
}

func newCreatePartitionEventData() *createPartitionEventData {
	return &createPartitionEventData{
		StartTimestamp: 0,
		EndTimestamp:   0,
	}
}

func newDropPartitionEventData() *dropPartitionEventData {
	return &dropPartitionEventData{
		StartTimestamp: 0,
		EndTimestamp:   0,
	}
}

func newIndexFileEventData() *indexFileEventData {
	return &indexFileEventData{
		StartTimestamp: 0,
		EndTimestamp:   0,
	}
}

func readInsertEventDataFixPart(buffer io.Reader) (*insertEventData, error) {
	data := &insertEventData{}
	if err := binary.Read(buffer, commonEndian, data); err != nil {
		return nil, err
	}
	return data, nil
}

func readDeleteEventDataFixPart(buffer io.Reader) (*deleteEventData, error) {
	data := &deleteEventData{}
	if err := binary.Read(buffer, commonEndian, data); err != nil {
		return nil, err
	}
	return data, nil
}

func readCreateCollectionEventDataFixPart(buffer io.Reader) (*createCollectionEventData, error) {
	data := &createCollectionEventData{}
	if err := binary.Read(buffer, commonEndian, data); err != nil {
		return nil, err
	}
	return data, nil
}

func readDropCollectionEventDataFixPart(buffer io.Reader) (*dropCollectionEventData, error) {
	data := &dropCollectionEventData{}
	if err := binary.Read(buffer, commonEndian, data); err != nil {
		return nil, err
	}
	return data, nil
}

func readCreatePartitionEventDataFixPart(buffer io.Reader) (*createPartitionEventData, error) {
	data := &createPartitionEventData{}
	if err := binary.Read(buffer, commonEndian, data); err != nil {
		return nil, err
	}
	return data, nil
}

func readDropPartitionEventDataFixPart(buffer io.Reader) (*dropPartitionEventData, error) {
	data := &dropPartitionEventData{}
	if err := binary.Read(buffer, commonEndian, data); err != nil {
		return nil, err
	}
	return data, nil
}

func readIndexFileEventDataFixPart(buffer io.Reader) (*indexFileEventData, error) {
	data := &indexFileEventData{}
	if err := binary.Read(buffer, commonEndian, data); err != nil {
		return nil, err
	}
	return data, nil
}
