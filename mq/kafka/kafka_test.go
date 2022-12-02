package kafka

import (
	"fmt"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
)

func TestConsumer(t *testing.T) {
	address := "localhost:9092"
	conf := kafka.ConfigMap{"bootstrap.servers": address, "acks": "all"}

	topic := fmt.Sprintf("t_%d", time.Now().UnixMilli())
	p, err := kafka.NewProducer(&conf)
	if err != nil {
		t.Fatal("create producer fail", err)
	}

	deliveryChan := make(chan kafka.Event, 1)
	for _, v := range []string{"1", "2", "3"} {
		m := &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0}, Value: []byte(v)}
		err := p.Produce(m, deliveryChan)
		if err != nil {
			t.Fatal("produce fail", err)
		}
		e, ok := <-deliveryChan
		if !ok {
			t.Fatal("produce fail", err)
		}

		km := e.(*kafka.Message)
		fmt.Println("send finished, msg offset:", km.TopicPartition.Offset)
	}

	c, err := NewKafkaConsumer(address, topic, "gid")
	if err != nil {
		t.Fatal("create consumer fail", err)
	}
	defer c.Close()

	msgID, err := c.GetLastMessageID()
	if err != nil {
		t.Fatal("GetLastMessageID fail", err)
	}
	assert.Equal(t, int64(2), msgID.(*kafkaID).messageID)

	msg, err := c.GetLastMessage()
	if err != nil {
		t.Fatal("GetLastMessage fail", err)
	}
	assert.Equal(t, "3", string(msg.Payload()))
}
