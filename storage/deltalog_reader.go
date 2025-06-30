package storage

import (
	"encoding/json"
	"io"

	binlogv1 "github.com/milvus-io/birdwatcher/storage/binlog/v1"
	"github.com/milvus-io/birdwatcher/storage/common"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

type DeltalogReader struct {
	reader io.Reader
}

func NewDeltalogReader(f common.ReadSeeker) (*DeltalogReader, error) {
	reader := &DeltalogReader{}

	// var de descriptorEvent
	var err error

	_, err = binlogv1.ReadMagicNumber(f)
	if err != nil {
		return nil, err
	}

	_, err = binlogv1.ReadDescriptorEvent(f)
	if err != nil {
		return nil, err
	}
	reader.reader = f
	return reader, nil
}

type DeleteLog struct {
	Pk     common.PrimaryKey `json:"pk"`
	Ts     uint64            `json:"ts"`
	PkType int64             `json:"pkType"`
}

func (dl *DeleteLog) UnmarshalJSON(data []byte) error {
	var messageMap map[string]*json.RawMessage
	err := json.Unmarshal(data, &messageMap)
	if err != nil {
		return err
	}

	err = json.Unmarshal(*messageMap["pkType"], &dl.PkType)
	if err != nil {
		return err
	}

	switch schemapb.DataType(dl.PkType) {
	case schemapb.DataType_Int64:
		dl.Pk = &common.Int64PrimaryKey{}
	case schemapb.DataType_VarChar:
		dl.Pk = &common.VarCharPrimaryKey{}
	}

	err = json.Unmarshal(*messageMap["pk"], dl.Pk)
	if err != nil {
		return err
	}

	err = json.Unmarshal(*messageMap["ts"], &dl.Ts)
	if err != nil {
		return err
	}

	return nil
}

func (dr *DeltalogReader) NextEventReader(dataType schemapb.DataType) (*DeltaData, error) {
	// TODO, improve logic for deltalog

	eventReader := binlogv1.NewEventReader()
	header, err := eventReader.ReadHeader(dr.reader)
	if err != nil {
		return nil, err
	}
	ifed, err := binlogv1.ReadIndexFileEventData(dr.reader)
	if err != nil {
		return nil, err
	}

	next := header.EventLength - header.GetMemoryUsageInBytes() - ifed.GetEventDataFixPartSize()
	data := make([]byte, next)
	_, err = io.ReadFull(dr.reader, data)
	if err != nil {
		return nil, err
	}

	pr, err := binlogv1.NewParquetPayloadReader(schemapb.DataType_String, data)
	if err != nil {
		return nil, err
	}

	entries, err := pr.GetBytesSliceFromPayload(0)
	if err != nil {
		return nil, err
	}
	deltaData := NewDeltaData(dataType, len(entries))
	var log DeleteLog
	for _, entry := range entries {
		err := json.Unmarshal(entry, &log)
		if err != nil {
			return nil, err
		}
		deltaData.Append(log.Pk, log.Ts)
	}
	return deltaData, nil
}
