package pulsar

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/birdwatcher/mq/ifc"
)

func TestConsumer(t *testing.T) {
	address := "pulsar://localhost:6650"
	topic := fmt.Sprintf("t_%d", time.Now().UnixMilli())

	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: address,
	})
	if err != nil {
		t.Fatal(err)
	}

	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: topic,
	})
	if err != nil {
		t.Fatal(err)
	}

	defer producer.Close()
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if msgID, err := producer.Send(ctx, &pulsar.ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		}); err != nil {
			log.Fatal(err)
		} else {
			log.Println("Published message: ", msgID)
		}
	}

	c, err := NewPulsarConsumer(address, topic, "gid", ifc.MqOption{
		SubscriptionInitPos: ifc.SubscriptionPositionLatest,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	msgID, err := c.GetLastMessageID()
	if err != nil {
		t.Fatal("GetLastMessageID fail", err)
	}
	assert.Equal(t, int64(4), msgID.(*pulsarID).messageID.EntryID())
	assert.Equal(t, int32(0), msgID.(*pulsarID).messageID.PartitionIdx())

	msg, err := c.GetLastMessage()
	if err != nil {
		t.Fatal("GetLastMessage fail", err)
	}
	assert.Equal(t, "hello-4", string(msg.Payload()))
}
