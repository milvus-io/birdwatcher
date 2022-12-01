package kafka

import (
	"fmt"

	"github.com/milvus-io/birdwatcher/mq/ifc"
)

type kafkaID struct {
	messageID int64
}

var _ ifc.MessageID = &kafkaID{}

func (kid *kafkaID) Serialize() []byte {
	return SerializeKafkaID(kid.messageID)
}

func (kid *kafkaID) AtEarliestPosition() bool {
	return kid.messageID <= 0
}

func (kid *kafkaID) Equal(msgID []byte) (bool, error) {
	return kid.messageID == DeserializeKafkaID(msgID), nil
}

func (kid *kafkaID) LessOrEqualThan(msgID []byte) (bool, error) {
	return kid.messageID <= DeserializeKafkaID(msgID), nil
}

func (kid *kafkaID) String() string {
	return fmt.Sprintf("messageID: %d", kid.messageID)
}

func SerializeKafkaID(messageID int64) []byte {
	b := make([]byte, 8)
	ifc.Endian.PutUint64(b, uint64(messageID))
	return b
}

func DeserializeKafkaID(messageID []byte) int64 {
	return int64(ifc.Endian.Uint64(messageID))
}
