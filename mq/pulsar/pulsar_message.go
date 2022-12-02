package pulsar

import (
	"github.com/apache/pulsar-client-go/pulsar"

	"github.com/milvus-io/birdwatcher/mq/ifc"
)

var _ ifc.Message = (*pulsarMessage)(nil)

type pulsarMessage struct {
	msg pulsar.Message
}

func (pm *pulsarMessage) Topic() string {
	return pm.msg.Topic()
}

func (pm *pulsarMessage) Properties() map[string]string {
	return pm.msg.Properties()
}

func (pm *pulsarMessage) Payload() []byte {
	return pm.msg.Payload()
}

func (pm *pulsarMessage) ID() ifc.MessageID {
	id := pm.msg.ID()
	pid := &pulsarID{messageID: id}
	return pid
}
