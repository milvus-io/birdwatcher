package reset

import (
	"encoding/base64"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	wplog "github.com/zilliztech/woodpecker/woodpecker/log"

	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
)

// The consume-checkpoint id we write must be decodable by the same unmarshaler
// Milvus uses. birdwatcher mirrors that logic in common.GetMessageIDString, so a
// round-trip through it proves the encoding is right rather than merely stable.
func TestWALPositionRoundTripsThroughDecoder(t *testing.T) {
	cases := []struct {
		wal        string
		decoderKey string
		wantDecode string
	}{
		{"woodpecker", "wp", "0/0"},
		{"wp", "wp", "0/0"},
		{"kafka", "kafka", "0"},
		{"rocksmq", "rmq", "0"},
	}

	for _, tc := range cases {
		t.Run(tc.wal, func(t *testing.T) {
			pos, err := buildWALPosition(tc.wal)
			require.NoError(t, err)

			got := common.GetMessageIDString(tc.decoderKey, pos.msgID.Id)
			assert.Equal(t, tc.wantDecode, got,
				"consume-checkpoint id must decode back to the expected position")
		})
	}
}

func TestWALPositionWoodpeckerEncodings(t *testing.T) {
	earliest, err := buildWALPosition("woodpecker")
	require.NoError(t, err)
	assert.Equal(t, commonpb.WALName_WoodPecker, earliest.walName)

	// raw bytes are what goes into msgpb.MsgPosition.MsgID
	id, err := wplog.DeserializeLogMessageId(earliest.raw)
	require.NoError(t, err)
	assert.Equal(t, int64(0), id.SegmentId)
	assert.Equal(t, int64(0), id.EntryId)

	// consume-checkpoint id is base64 over exactly those bytes
	decoded, err := base64.StdEncoding.DecodeString(earliest.msgID.Id)
	require.NoError(t, err)
	assert.Equal(t, earliest.raw, decoded)
	assert.Equal(t, commonpb.WALName_WoodPecker, earliest.msgID.WALName)
}

func TestWALPositionInt64RawIsLittleEndian(t *testing.T) {
	pos, err := buildWALPosition("kafka")
	require.NoError(t, err)
	require.Len(t, pos.raw, 8)
	assert.Equal(t, int64(0), int64(binary.LittleEndian.Uint64(pos.raw)))
}

func TestWALPositionRejectsUnknownWAL(t *testing.T) {
	_, err := buildWALPosition("nats")
	assert.ErrorContains(t, err, "unsupported --target-wal")

	_, err = buildWALPosition("")
	assert.ErrorContains(t, err, "--target-wal is required")
}

// woodpecker's earliest id serializes to zero bytes, because protobuf omits
// zero-valued scalars. Milvus relies on this: msgdispatcher only treats an empty
// MsgID as seekable when WALName says WoodPecker (pkg/mq/msgdispatcher/
// dispatcher.go). So the empty encoding is correct, but only if we also set
// WALName — which is exactly what birdcatcher's rename path forgets to do.
func TestWoodpeckerEarliestIsEmptyBytesButNamed(t *testing.T) {
	pos, err := buildWALPosition("woodpecker")
	require.NoError(t, err)

	assert.Empty(t, pos.raw, "woodpecker earliest serializes to zero bytes")
	assert.Empty(t, pos.msgID.Id, "and therefore to an empty base64 string")
	assert.Equal(t, commonpb.WALName_WoodPecker, pos.walName,
		"WALName must still be set, or milvus cannot tell an empty id from an unset one")
	assert.Equal(t, commonpb.WALName_WoodPecker, pos.msgID.WALName)
}
