package binlogv1

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

func TestIndexReaderReadsRawPayload(t *testing.T) {
	payload := []byte{1, 2, 3, 4, 5}
	file := buildRawIndexFile(t, payload)
	readerAtEvent := bytes.NewReader(file)

	reader, descriptor, err := NewIndexReader(readerAtEvent)
	require.NoError(t, err)
	require.Equal(t, schemapb.DataType_None, descriptor.PayloadDataType)

	actual, err := reader.NextRawEventReader(readerAtEvent)
	require.NoError(t, err)
	require.Equal(t, payload, actual)
}

func TestIndexReaderRejectsTruncatedRawPayload(t *testing.T) {
	payload := []byte{1, 2, 3, 4, 5}
	file := buildRawIndexFile(t, payload)
	offset := descriptorFileOffset(t, file)

	_, err := (&IndexReader{}).NextRawEventReader(bytes.NewReader(file[offset : len(file)-1]))
	require.Error(t, err)
}

func buildRawIndexFile(t *testing.T, payload []byte) []byte {
	t.Helper()

	descriptorData := newDescriptorEventData()
	descriptorData.PayloadDataType = schemapb.DataType_None
	descriptorData.Extras[originalSizeKey] = "5"
	require.NoError(t, descriptorData.FinishExtra())

	descriptorHeader := newDescriptorEventHeader()
	descriptorHeader.EventLength = descriptorHeader.GetMemoryUsageInBytes() + descriptorData.GetMemoryUsageInBytes()
	descriptorHeader.NextPosition = int32(binary.Size(MagicNumberV1)) + descriptorHeader.EventLength

	indexData := newIndexFileEventData()
	indexData.SetEventTimestamp(1, 1)
	indexHeader := newEventHeader(IndexFileEventType)
	indexHeader.EventLength = indexHeader.GetMemoryUsageInBytes() + indexData.GetEventDataFixPartSize() + int32(len(payload))
	indexHeader.NextPosition = descriptorHeader.NextPosition + indexHeader.EventLength

	var output bytes.Buffer
	require.NoError(t, binary.Write(&output, commonEndian, MagicNumberV1))
	require.NoError(t, descriptorHeader.Write(&output))
	require.NoError(t, descriptorData.Write(&output))
	require.NoError(t, indexHeader.Write(&output))
	require.NoError(t, indexData.WriteEventData(&output))
	_, err := output.Write(payload)
	require.NoError(t, err)
	return output.Bytes()
}

func descriptorFileOffset(t *testing.T, file []byte) int {
	t.Helper()
	reader := bytes.NewReader(file)
	_, _, err := NewIndexReader(reader)
	require.NoError(t, err)
	offset, err := reader.Seek(0, 1)
	require.NoError(t, err)
	return int(offset)
}
