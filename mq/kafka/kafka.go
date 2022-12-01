package kafka

import (
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"

	"github.com/milvus-io/birdwatcher/mq/ifc"
)

type Consumer struct {
	topic string
	c     *kafka.Consumer
}

func NewKafkaConsumer(address, topic, groupID string) (*Consumer, error) {
	config := &kafka.ConfigMap{
		"bootstrap.servers":   address,
		"api.version.request": true,
		"group.id":            groupID,
	}
	c, err := kafka.NewConsumer(config)
	if err != nil {
		return nil, err
	}

	return &Consumer{topic: topic, c: c}, nil
}

func (k *Consumer) GetLastMessageID() (ifc.MessageID, error) {
	low, high, err := k.c.QueryWatermarkOffsets(k.topic, ifc.DefaultPartitionIdx, 1200)
	if err != nil {
		return nil, nil
	}

	if high > 0 {
		high = high - 1
	}
	fmt.Printf("get latest msgID, low offset:%d, high offset:%d\n", low, high)
	return &kafkaID{messageID: high}, nil
}

func (k *Consumer) GetLastMessage() (ifc.Message, error) {
	fmt.Printf("start read the latest msg from topic:%s\n", k.topic)
	err := k.c.Assign([]kafka.TopicPartition{{Topic: &k.topic, Partition: ifc.DefaultPartitionIdx, Offset: kafka.OffsetTail(1)}})
	if err != nil {
		return nil, err
	}
	e, err := k.c.ReadMessage(30 * time.Second)
	if err != nil {
		return nil, err
	}

	fmt.Printf("read the latest msg successfully from topic:%s, message offset, %s\n", k.topic, e.TopicPartition.Offset)
	return &kafkaMessage{msg: e}, nil
}

func (k *Consumer) Close() error {
	return k.c.Close()
}
