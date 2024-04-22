package pulsar

import (
	"context"
	"errors"
	"fmt"

	"github.com/apache/pulsar-client-go/pulsar"

	"github.com/milvus-io/birdwatcher/mq/ifc"
)

type pulsarConsumer struct {
	topic    string
	consumer pulsar.Consumer
	client   pulsar.Client
}

func NewPulsarConsumer(address string, topic string, groupID string, config ifc.MqOption) (*pulsarConsumer, error) {
	client, err := pulsar.NewClient(pulsar.ClientOptions{URL: address})
	if err != nil {
		return nil, err
	}

	var initPos pulsar.SubscriptionInitialPosition
	switch config.SubscriptionInitPos {
	case ifc.SubscriptionPositionEarliest:
		initPos = pulsar.SubscriptionPositionEarliest
	case ifc.SubscriptionPositionLatest:
		initPos = pulsar.SubscriptionPositionLatest
	}

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            groupID,
		Type:                        pulsar.Exclusive,
		SubscriptionInitialPosition: initPos,
	})
	if err != nil {
		return nil, err
	}

	return &pulsarConsumer{topic: topic, consumer: consumer, client: client}, nil
}

func (p *pulsarConsumer) Consume() (ifc.Message, error) {
	msg := <-p.consumer.Chan()
	p.consumer.Ack(msg)
	return &pulsarMessage{msg: msg}, nil
}

func (p *pulsarConsumer) GetLastMessageID() (ifc.MessageID, error) {
	msgID, err := p.consumer.GetLastMessageID(p.topic, 0)
	return &pulsarID{messageID: msgID}, err
}

func (p *pulsarConsumer) GetLastMessage() (ifc.Message, error) {
	msgID, err := p.consumer.GetLastMessageID(p.topic, 0)
	if err != nil {
		return nil, err
	}
	reader, err := p.client.CreateReader(
		pulsar.ReaderOptions{
			Topic:                   p.topic,
			StartMessageID:          msgID,
			StartMessageIDInclusive: true,
		})
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	ctx := context.Background()
	if reader.HasNext() {
		fmt.Printf("start read the latest msg from topic:%s\n", p.topic)
		msg, err := reader.Next(ctx)
		if err != nil {
			return nil, err
		}
		pid := &pulsarID{messageID: msg.ID()}
		fmt.Printf("read the latest msg successfully from topic:%s, message offset, %s\n", p.topic, pid.String())
		return &pulsarMessage{msg: msg}, nil
	}

	return nil, errors.New("not found latest message, topic:" + p.topic)
}

func (p *pulsarConsumer) Close() error {
	p.consumer.Close()
	p.client.Close()
	return nil
}
