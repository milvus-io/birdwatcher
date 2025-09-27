package states

import (
	"context"
	"fmt"
	"os"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

type CmpPChannelsParam struct {
	framework.ParamBase `use:"cmp_pchannels" desc:"compare difference between pchannels"`
	WALName             string   `name:"wal_name" default:"Pulsar" desc:"wal name to consume"`
	PChannels           []string `name:"pchannels" default:"" desc:"the pchannels to compare"`
	MQAddrs             []string `name:"mq_addrs" default:"" desc:"mq addresses for cmp (multiple addresses, one per pchannel)"`
}

func (s *InstanceState) CmpPChannelsCommand(ctx context.Context, p *CmpPChannelsParam) error {
	pchannels := p.PChannels
	if len(pchannels) < 2 {
		return errors.New("at least 2 pchannels are required for comparison")
	}

	// Setup signal handling for Ctrl+C
	sigChan := SetupSignalHandling()
	defer CleanupSignalHandling(sigChan)
	fmt.Println("Starting message comparison. Press Ctrl+C to stop...")

	// Create scanners for all pchannels
	scanners := make([]*WALScanner, len(pchannels))
	for i, channel := range pchannels {
		// Use empty string if no mq addresses provided, otherwise use the corresponding address
		mqAddr := ""
		if len(p.MQAddrs) > i {
			mqAddr = p.MQAddrs[i]
		}
		scanner, err := NewWALScanner(ctx, p.WALName, channel, mqAddr)
		if err != nil {
			return errors.Wrapf(err, "failed to create scanner for channel %s", channel)
		}
		scanners[i] = scanner
		defer scanner.Scanner.Close()
	}

	// Start the simplified comparison process
	return s.comparePChannelsMessage(ctx, scanners, pchannels, sigChan)
}

// ChannelMessage represents a message from a specific pchannel
type ChannelMessage struct {
	PChannelName string
	Message      message.ImmutableMessage
}

func (s *InstanceState) comparePChannelsMessage(
	ctx context.Context,
	scanners []*WALScanner,
	pchannelNames []string,
	sigChan chan os.Signal,
) error {
	// Create channels for collecting messages from all scanners
	messageChannels := make([]<-chan message.ImmutableMessage, len(scanners))
	for i, scanner := range scanners {
		messageChannels[i] = scanner.MessageChan
	}

	// Track current message for each pchannel (only one at a time)
	currentMessages := make(map[string]message.ImmutableMessage)
	messageCounters := make(map[string]int)
	readyForComparison := make(map[string]bool)

	// Initialize counters and ready flags
	for _, pchannelName := range pchannelNames {
		messageCounters[pchannelName] = 0
		readyForComparison[pchannelName] = false
	}

	// Process messages and perform comparison
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-sigChan:
			return nil
		default:
			// Try to read one message from each pchannel that's not ready
			allReady := true
			for i, msgChan := range messageChannels {
				pchannelName := pchannelNames[i]

				// Skip if this pchannel is already ready for comparison
				if readyForComparison[pchannelName] {
					continue
				}

				// Try to read one message from this pchannel
				select {
				case msg, ok := <-msgChan:
					if !ok {
						// This pchannel is closed
						continue
					}
					if msg.MessageType().IsSelfControlled() {
						continue
					}

					// Store the current message for this pchannel
					currentMessages[pchannelName] = msg
					messageCounters[pchannelName]++
					readyForComparison[pchannelName] = true

				default:
					// No message available from this pchannel
					allReady = false
				}
			}

			// Check if all pchannels are ready for comparison
			if allReady {
				for _, pchannelName := range pchannelNames {
					if !readyForComparison[pchannelName] {
						allReady = false
						break
					}
				}
			}

			// Perform comparison when all pchannels are ready
			if allReady {
				// Compare the current messages from all pchannels
				if !s.compareCurrentMessages(currentMessages, pchannelNames) {
					fmt.Printf("❌ INCONSISTENCY DETECTED!\n")

					s.printInconsistentCurrentMessages(currentMessages, pchannelNames)
					return errors.New("message inconsistency detected between pchannels")
				}

				// Display messages after successful comparison
				s.displayComparedCurrentMessages(currentMessages, pchannelNames, messageCounters)

				// Reset for next round of comparison
				for _, pchannelName := range pchannelNames {
					readyForComparison[pchannelName] = false
					delete(currentMessages, pchannelName)
				}
			}
		}
	}
}

func (s *InstanceState) compareCurrentMessages(
	currentMessages map[string]message.ImmutableMessage,
	pchannelNames []string,
) bool {
	getMessageID := func(msg message.ImmutableMessage) string {
		if msg.ReplicateHeader() != nil {
			return msg.ReplicateHeader().MessageID.String()
		}
		return msg.MessageID().String()
	}

	// Use first pchannel as reference
	referencePChannel := pchannelNames[0]
	referenceMsg := currentMessages[referencePChannel]
	referenceMsgID := getMessageID(referenceMsg)

	// Compare all other pchannels with the reference
	for i := 1; i < len(pchannelNames); i++ {
		pchannelName := pchannelNames[i]
		pchannelMsg := currentMessages[pchannelName]

		if referenceMsg.MessageType() == message.MessageTypeAlterReplicateConfig {
			// only validate message type for AlterReplicateConfigMsg, because the
			// secondary milvus may append AlterReplicateConfigMsg itself.
			if pchannelMsg.MessageType() != message.MessageTypeAlterReplicateConfig {
				return false
			}
		} else {
			if referenceMsg.MessageType() != pchannelMsg.MessageType() ||
				referenceMsgID != getMessageID(pchannelMsg) {
				return false
			}
		}
	}

	return true
}

func (s *InstanceState) displayComparedCurrentMessages(
	currentMessages map[string]message.ImmutableMessage,
	pchannelNames []string,
	messageCounters map[string]int,
) {
	getMessageInfo := func(pchannelNames []string,
		currentMessages map[string]message.ImmutableMessage,
		messageCounters map[string]int,
	) ([]string, []string, []int) {
		messageIDs := make([]string, len(pchannelNames))
		timeTicks := make([]string, len(pchannelNames))
		counts := make([]int, len(pchannelNames))
		for i, pchannelName := range pchannelNames {
			message := currentMessages[pchannelName]
			counts[i] = messageCounters[pchannelName]
			messageIDs[i] = message.MessageID().String()
			timeTicks[i] = fmt.Sprintf("%d", message.TimeTick())
			if message.ReplicateHeader() != nil {
				messageIDs[i] = fmt.Sprintf("%s(r:%s)", messageIDs[i], message.ReplicateHeader().MessageID.String())
				timeTicks[i] = fmt.Sprintf("%s(r:%d)", timeTicks[i], message.ReplicateHeader().TimeTick)
			}
		}
		return messageIDs, timeTicks, counts
	}

	if len(pchannelNames) > 0 && currentMessages[pchannelNames[0]] != nil {
		msg := currentMessages[pchannelNames[0]]
		messageIDs, timeTicks, counts := getMessageInfo(pchannelNames, currentMessages, messageCounters)
		fmt.Printf("✅ [Type=%s] [MessageIDs=%s] [TimeTicks=%v] [PChannels=%v] [Counts=%v]\n",
			msg.MessageType().String(),
			messageIDs,
			timeTicks,
			pchannelNames,
			counts)
	}
}

func (s *InstanceState) printInconsistentCurrentMessages(
	currentMessages map[string]message.ImmutableMessage,
	pchannelNames []string,
) {
	fmt.Println("=== INCONSISTENT MESSAGES ===")
	for _, pchannelName := range pchannelNames {
		if msg, exists := currentMessages[pchannelName]; exists {
			fmt.Printf("PChannel %s: %s\n",
				pchannelName,
				FormatMessageInfo(msg))
		}
	}
	fmt.Println("=============================")
}
