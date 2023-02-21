//go:build WKAFKA
// +build WKAFKA

package mq

import (
	"fmt"
	"time"

	"github.com/milvus-io/birdwatcher/mq/ifc"
	"github.com/milvus-io/birdwatcher/mq/kafka"
	"github.com/milvus-io/birdwatcher/mq/pulsar"
)

func NewConsumer(mqType, address, channel string) (ifc.Consumer, error) {
	groupID := fmt.Sprintf("group-id-%d", time.Now().UnixNano())
	switch mqType {
	case "kafka":
		return kafka.NewKafkaConsumer(address, channel, groupID)
	case "pulsar":
		return pulsar.NewPulsarConsumer(address, channel, groupID)
	default:
		panic("unknown mq type:" + mqType)
	}
}
