package reset

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/cockroachdb/errors"
	wplog "github.com/zilliztech/woodpecker/woodpecker/log"

	bwpulsar "github.com/milvus-io/birdwatcher/mq/pulsar"
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// walPosition is the target WAL's earliest position, expressed in the two
// encodings Milvus persists:
//
//	raw   -> msgpb.MsgPosition.MsgID        (channel-cp, segment & collection positions)
//	msgID -> commonpb.MessageID             (streamingnode consume-checkpoint)
//
// Earliest is a sentinel the broker resolves, so it can be built offline without
// connecting to the target MQ.
//
// There is deliberately no "latest" counterpart. A persisted checkpoint is always
// consumed through DeliverPolicyStartFrom (see the delegator adaptor in
// internal/distributed/streaming/msgstream_adaptor.go), never as a DeliverPolicy_Latest
// policy — so a "latest" sentinel written here becomes a position that does not
// exist, and the reader tails it forever. Skipping stale data on the target topic
// is already handled: Milvus filters the stream with DeliverFilterTimeTickGTE
// against the timestamp we preserve, so starting from earliest cannot replay
// anything older than the checkpoint.
type walPosition struct {
	walName commonpb.WALName
	raw     []byte
	msgID   *commonpb.MessageID
}

func (p *walPosition) String() string {
	return fmt.Sprintf("%s/earliest", p.walName.String())
}

// buildWALPosition resolves --target-wal into the earliest position of that WAL.
func buildWALPosition(targetWAL string) (*walPosition, error) {
	wal := strings.ToLower(strings.TrimSpace(targetWAL))
	if wal == "wp" {
		wal = "woodpecker"
	}

	switch wal {
	case "woodpecker":
		id := &wplog.LogMessageId{SegmentId: 0, EntryId: 0}
		return newBase64Position(commonpb.WALName_WoodPecker, id.Serialize()), nil

	case "pulsar":
		return newBase64Position(commonpb.WALName_Pulsar,
			bwpulsar.SerializePulsarMsgID(pulsar.EarliestMessageID())), nil

	case "kafka":
		// Offset 0 rather than the OffsetBeginning sentinel (-2): kafkaID.Marshal
		// encodes with EncodeInt64 while unmarshalMessageID decodes with
		// DecodeUint64, which rejects the minus sign (pkg/v2 walimpls/impls/kafka/
		// message_id.go). A negative offset written here would be unreadable. On
		// the fresh topic this command targets, 0 is the earliest offset.
		return newInt64Position(commonpb.WALName_Kafka, 0), nil

	case "rocksmq", "rockmq":
		return newInt64Position(commonpb.WALName_RocksMQ, 0), nil

	case "":
		return nil, errors.New("--target-wal is required (woodpecker, pulsar, kafka, rocksmq)")
	default:
		return nil, errors.Newf("unsupported --target-wal %q, expect one of woodpecker, pulsar, kafka, rocksmq", targetWAL)
	}
}

// newBase64Position builds a position whose consume-checkpoint encoding is
// base64 over the same bytes stored raw in MsgPosition.MsgID.
func newBase64Position(name commonpb.WALName, raw []byte) *walPosition {
	return &walPosition{
		walName: name,
		raw:     raw,
		msgID: &commonpb.MessageID{
			WALName: name,
			Id:      base64.StdEncoding.EncodeToString(raw),
		},
	}
}

// newInt64Position builds a position for the MQs whose message id is a bare
// int64 (kafka offset, rocksmq sequence).
func newInt64Position(name commonpb.WALName, value int64) *walPosition {
	return &walPosition{
		walName: name,
		raw:     serializeInt64(value),
		msgID: &commonpb.MessageID{
			WALName: name,
			Id:      message.EncodeInt64(value),
		},
	}
}

// serializeInt64 encodes a bare int64 message id the same way Milvus does for
// kafka and rocksmq (8 bytes, little endian) — see pkg/v2 mqwrapper
// SerializeKafkaID / SerializeRmqID.
func serializeInt64(value int64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(value))
	return b
}
