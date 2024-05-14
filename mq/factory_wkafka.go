//go:build WKAFKA
// +build WKAFKA

package mq

import (
	"fmt"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/birdwatcher/mq/ifc"
	"github.com/milvus-io/birdwatcher/mq/kafka"
	"github.com/milvus-io/birdwatcher/mq/pulsar"
)

func ParsePositionFromCheckpoint(mqType string, messageID []byte) (ifc.MessageID, error) {
	switch mqType {
	case "pulsar":
		return pulsar.DeserializePulsarMsgID(messageID)
	case "kafka":
		return kafka.DeserializeKafkaID(messageID), nil
	default:
		return nil, errors.Newf("not supported mq type: %s", mqType)
	}
}

func ParseManualMessageID(mqType string, manualID int64) (ifc.MessageID, error) {
	switch mqType {
	// pulsar not supported yet
	case "kafka":
		return kafka.DeserializeKafkaID(kafka.SerializeKafkaID(manualID)), nil
	default:
		return nil, errors.Newf("not supported mq type: %s", mqType)
	}
}

func NewConsumer(mqType, address, channel string, config ifc.MqOption) (ifc.Consumer, error) {
	groupID := fmt.Sprintf("group-id-%d", time.Now().UnixNano())
	switch mqType {
	case "kafka":
		return kafka.NewKafkaConsumer(address, channel, groupID, config)
	case "pulsar":
		return pulsar.NewPulsarConsumer(address, channel, groupID, config)
	default:
		panic("unknown mq type:" + mqType)
	}
}
