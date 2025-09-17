package states

import (
	"context"
	"fmt"
	"math"
	"os"
	"strconv"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

type ConsumeV2Param struct {
	framework.ParamBase `use:"consumev2" desc:"consume msgs from mq"`
	WALName             string   `name:"wal_name" default:"Pulsar" desc:"wal name to consume"`
	PChannel            string   `name:"pchannel" desc:"the pchannel to consume"`
	Limit               string   `name:"limit" default:"-1" desc:"limit the number of messages to consume"`
	CmpDiff             bool     `name:"cmp_diff" default:"false" desc:"compare difference between pchannels"`
	CmpPChannels        []string `name:"cmp_pchannels" default:"" desc:"the pchannels to compare"`
}

func (s *InstanceState) ConsumeV2Command(ctx context.Context, p *ConsumeV2Param) error {
	// Setup signal handling for Ctrl+C
	sigChan := SetupSignalHandling()
	defer CleanupSignalHandling(sigChan)
	fmt.Println("Starting message consumption. Press Ctrl+C to stop...")

	if p.CmpDiff {
		return s.compareMode(ctx, p, sigChan)
	} else {
		return s.consumeMode(ctx, p, sigChan)
	}
}

func (s *InstanceState) consumeMode(ctx context.Context, p *ConsumeV2Param, sigChan chan os.Signal) error {
	scanner, err := NewWALScanner(ctx, p.WALName, p.PChannel)
	if err != nil {
		return err
	}
	defer scanner.Scanner.Close()

	idx := 0
	limit := 0
	if p.Limit == "-1" {
		limit = math.MaxInt
	} else {
		limit, err = strconv.Atoi(p.Limit)
		if err != nil {
			return err
		}
	}
	count := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-sigChan:
			return nil
		case <-time.After(200 * time.Millisecond):
			ShowSpinner(idx)
			idx++
		case msg, ok := <-scanner.MessageChan:
			if !ok {
				return errors.New("scanner closed")
			}
			if msg.MessageType().IsSelfControlled() {
				continue
			}
			fmt.Print("\r\033[K")
			fmt.Printf("📥%s\n", FormatMessageInfo(msg))
			count++
			if count >= limit {
				fmt.Printf("Reached limit %d, stopping consumption.\n", limit)
				return nil
			}
		}
	}
}

func (s *InstanceState) compareMode(ctx context.Context, p *ConsumeV2Param, sigChan chan os.Signal) error {
	pchannels := p.CmpPChannels
	if len(pchannels) < 2 {
		return errors.New("at least 2 pchannels are required for comparison")
	}

	// Create scanners for all pchannels
	scanners := make([]*WALScanner, len(pchannels))
	for i, channel := range pchannels {
		scanner, err := NewWALScanner(ctx, p.WALName, channel)
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
	idx := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-sigChan:
			return nil
		case <-time.After(200 * time.Millisecond):
			ShowSpinner(idx)
			idx++
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
				fmt.Print("\r\033[K")

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

		if referenceMsg.MessageType() == message.MessageTypePutReplicateConfig {
			// only validate message type for PutReplicateConfigMsg, because the
			// secondary milvus may append PutReplicateConfigMsg itself.
			if pchannelMsg.MessageType() != message.MessageTypePutReplicateConfig {
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
	getCounts := func(pchannelNames []string, messageCounters map[string]int) []int {
		counts := make([]int, len(pchannelNames))
		for i, pchannelName := range pchannelNames {
			counts[i] = messageCounters[pchannelName]
		}
		return counts
	}

	if len(pchannelNames) > 0 && currentMessages[pchannelNames[0]] != nil {
		msg := currentMessages[pchannelNames[0]]
		counts := getCounts(pchannelNames, messageCounters)
		fmt.Printf("✅ [Type=%s] [MessageID=%s] [PChannels=%v] [Counts=%v]\n",
			msg.MessageType().String(),
			msg.MessageID().String(),
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
