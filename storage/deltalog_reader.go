package storage

import (
	"encoding/json"
	"io"

	"github.com/milvus-io/birdwatcher/proto/v2.0/schemapb"
)

type DeltalogReader struct {
	reader io.Reader
}

func NewDeltalogReader(f ReadSeeker) (*DeltalogReader, error) {
	reader := &DeltalogReader{}

	// var de descriptorEvent
	var err error

	_, err = readMagicNumber(f)
	if err != nil {
		return nil, err
	}

	_, err = ReadDescriptorEvent(f)
	if err != nil {
		return nil, err
	}
	reader.reader = f
	return reader, nil
}

type DeleteLog struct {
	Pk     PrimaryKey `json:"pk"`
	Ts     uint64     `json:"ts"`
	PkType int64      `json:"pkType"`
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
		dl.Pk = &Int64PrimaryKey{}
	case schemapb.DataType_VarChar:
		dl.Pk = &VarCharPrimaryKey{}
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
	eventReader := newEventReader()
	header, err := eventReader.readHeader(dr.reader)
	if err != nil {
		return nil, err
	}
	ifed, err := readIndexFileEventData(dr.reader)
	if err != nil {
		return nil, err
	}

	next := header.EventLength - header.GetMemoryUsageInBytes() - ifed.GetEventDataFixPartSize()
	data := make([]byte, next)
	_, err = io.ReadFull(dr.reader, data)
	if err != nil {
		return nil, err
	}

	pr, err := NewParquetPayloadReader(schemapb.DataType_String, data)
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
