package states

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/birdwatcher/wal/adaptor"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/registry"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
)

const messageCipherHeader = "_ch"

// WALScanner represents a scanner for a single pchannel
type WALScanner struct {
	ChannelName string
	Scanner     adaptor.Scanner
	MessageChan <-chan message.ImmutableMessage
}

// NewWALScanner creates a new WAL scanner for a given pchannel
func NewWALScanner(ctx context.Context, walName, topic string, mqAddr string) (*WALScanner, error) {
	mqIP := mqAddr
	mqPort := ""
	if host, port, err := net.SplitHostPort(mqAddr); err == nil {
		mqIP = host
		mqPort = port
	}

	walNameEnum := message.WALNamePulsar
	switch walName {
	case commonpb.WALName_Pulsar.String():
		if mqIP != "" {
			paramtable.Get().Save(paramtable.Get().PulsarCfg.Address.Key, mqIP)
			defer paramtable.Get().Reset(paramtable.Get().PulsarCfg.Address.Key)
		}
		if mqPort != "" {
			paramtable.Get().Save(paramtable.Get().PulsarCfg.Port.Key, mqPort)
			defer paramtable.Get().Reset(paramtable.Get().PulsarCfg.Port.Key)
		}
	case commonpb.WALName_Kafka.String():
		return nil, errors.Newf("kafka is not supported yet")
	case commonpb.WALName_RocksMQ.String():
		return nil, errors.Newf("rocksmq is not supported yet")
	case commonpb.WALName_WoodPecker.String():
		return nil, errors.Newf("woodpecker is not supported yet")
	default:
		return nil, errors.Newf("invalid wal name: %s", walName)
	}

	b := registry.MustGetBuilder(walNameEnum)
	opener, err := b.Build()
	if err != nil {
		return nil, err
	}

	wal, err := opener.Open(ctx, &walimpls.OpenOption{
		Channel: types.PChannelInfo{
			Name:       topic,
			AccessMode: types.AccessModeRO,
		},
	})
	if err != nil {
		return nil, err
	}

	readOption := adaptor.ReadOption{
		DeliverPolicy: options.DeliverPolicyAll(),
	}
	scanner := adaptor.NewScanner(wal, readOption)
	msgChan := scanner.Chan()

	return &WALScanner{
		ChannelName: topic,
		Scanner:     scanner,
		MessageChan: msgChan,
	}, nil
}

// FormatMessageInfo formats message information for display
func FormatMessageInfo(msg message.ImmutableMessage) string {
	if msg.ReplicateHeader() != nil {
		return fmt.Sprintf(
			"[Type=%s] [VChannel=%s] [TimeTick=%d] [Time=%v] [MessageID=%s] [ReplicateMessageID=%s] [Size=%d]",
			msg.MessageType().String(),
			msg.VChannel(),
			msg.TimeTick(),
			tsoutil.PhysicalTime(msg.TimeTick()),
			msg.MessageID().String(),
			msg.ReplicateHeader().MessageID.String(),
			msg.EstimateSize(),
		)
	}
	cipherHeader := ""
	if cipherProperty, ok := msg.Properties().Get(messageCipherHeader); ok {
		header := &messagespb.CipherHeader{}
		if err := message.DecodeProto(cipherProperty, header); err == nil {
			cipherHeader = header.String()
		}
	}
	return fmt.Sprintf(
		"[Type=%s] [VChannel=%s] [TimeTick=%d] [Time=%v] [MessageID=%s] [Size=%d] [CipherHeader=%v]",
		msg.MessageType().String(),
		msg.VChannel(),
		msg.TimeTick(),
		tsoutil.PhysicalTime(msg.TimeTick()),
		msg.MessageID().String(),
		msg.EstimateSize(),
		cipherHeader,
	)
}

// SetupSignalHandling sets up signal handling for graceful shutdown
func SetupSignalHandling() chan os.Signal {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT)
	return sigChan
}

// CleanupSignalHandling cleans up signal handling
func CleanupSignalHandling(sigChan chan os.Signal) {
	signal.Stop(sigChan)
}
