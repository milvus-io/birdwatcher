package adaptor

import (
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/options"
)

type ReadOption struct {
	VChannel string // vchannel is a optional field to select a vchannel to consume.
	// If the vchannel is setup, the message that is not belong to these vchannel will be dropped by scanner.
	// Otherwise all message on WAL will be sent.
	DeliverPolicy  options.DeliverPolicy
	MessageFilter  []options.DeliverFilter
	MesasgeHandler message.Handler // message handler for message processing.
	// If the message handler is nil (no redundant operation need to apply),
	// the default message handler will be used, and the receiver will be returned from Chan.
	// Otherwise, Chan will panic.
	// vaild every message will be passed to this handler before being delivered to the consumer.
}
