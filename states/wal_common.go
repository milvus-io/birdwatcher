package states

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/birdwatcher/wal/adaptor"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls"
	_ "github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/pulsar"
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

	var walNameEnum message.WALName
	switch walName {
	case commonpb.WALName_Pulsar.String(), strings.ToLower(commonpb.WALName_Pulsar.String()):
		walNameEnum = message.WALNamePulsar
		if mqIP != "" {
			paramtable.Get().Save(paramtable.Get().PulsarCfg.Address.Key, mqIP)
			defer paramtable.Get().Reset(paramtable.Get().PulsarCfg.Address.Key)
		}
		if mqPort != "" {
			paramtable.Get().Save(paramtable.Get().PulsarCfg.Port.Key, mqPort)
			defer paramtable.Get().Reset(paramtable.Get().PulsarCfg.Port.Key)
		}
	case commonpb.WALName_Kafka.String(), strings.ToLower(commonpb.WALName_Kafka.String()):
		walNameEnum = message.WALNameKafka
		// port is not set, use default port 9092
		if mqAddr != "" && mqPort == "" {
			addr := fmt.Sprintf("%s:%d", mqAddr, 9092)
			paramtable.Get().Save(paramtable.Get().KafkaCfg.Address.Key, addr)
			defer paramtable.Get().Reset(paramtable.Get().KafkaCfg.Address.Key)
		}
		// port is set in mqAddr
		if mqAddr != "" && mqPort != "" {
			paramtable.Get().Save(paramtable.Get().KafkaCfg.Address.Key, mqAddr)
			defer paramtable.Get().Reset(paramtable.Get().KafkaCfg.Address.Key)
		}
	case commonpb.WALName_RocksMQ.String(), strings.ToLower(commonpb.WALName_RocksMQ.String()):
		return nil, errors.Newf("rocksmq is not supported yet")
	case commonpb.WALName_WoodPecker.String(), strings.ToLower(commonpb.WALName_WoodPecker.String()):
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
	parts := []string{
		fmt.Sprintf("[Type=%s]", msg.MessageType().String()),
		fmt.Sprintf("[VChannel=%s]", msg.VChannel()),
		fmt.Sprintf("[TimeTick=%d]", msg.TimeTick()),
		fmt.Sprintf("[Time=%v]", tsoutil.PhysicalTime(msg.TimeTick())),
		fmt.Sprintf("[MessageID=%s]", msg.MessageID().String()),
	}

	if msg.ReplicateHeader() != nil {
		parts = append(parts, fmt.Sprintf("[rVChannel=%s]", msg.ReplicateHeader().VChannel))
		parts = append(parts, fmt.Sprintf("[rTimeTick=%d]", msg.ReplicateHeader().TimeTick))
		parts = append(parts, fmt.Sprintf("[rTime=%v]", tsoutil.PhysicalTime(msg.ReplicateHeader().TimeTick)))
		parts = append(parts, fmt.Sprintf("[rMessageID=%s]", msg.ReplicateHeader().MessageID.String()))
	}

	parts = append(parts, fmt.Sprintf("[Size=%d]", msg.EstimateSize()))

	if cipherProperty, ok := msg.Properties().Get(messageCipherHeader); ok {
		header := &messagespb.CipherHeader{}
		if err := message.DecodeProto(cipherProperty, header); err == nil {
			cipherHeaderStr := header.String()
			if cipherHeaderStr != "" && cipherHeaderStr != "[]" {
				parts = append(parts, fmt.Sprintf("[CipherHeader=%s]", cipherHeaderStr))
			}
		}
	}

	return strings.Join(parts, " ")
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
