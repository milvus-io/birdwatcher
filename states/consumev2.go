package states

import (
	"context"
	"fmt"
	"math"
	"strconv"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/birdwatcher/framework"
)

type ConsumeV2Param struct {
	framework.ParamBase `use:"consumev2" desc:"consume msgs from mq"`
	WALName             string `name:"wal_name" default:"Pulsar" desc:"wal name to consume"`
	PChannel            string `name:"pchannel" desc:"the pchannel to consume"`
	Limit               string `name:"limit" default:"-1" desc:"limit the number of messages to consume"`
	MQAddr              string `name:"mq_addr" default:"" desc:"mq address for consume mode (single address)"`
	// TODO: sheep, support consume by messageID
}

func (s *InstanceState) ConsumeV2Command(ctx context.Context, p *ConsumeV2Param) error {
	if p.PChannel == "" {
		return errors.New("pchannel must be provided")
	}

	// Setup signal handling for Ctrl+C
	sigChan := SetupSignalHandling()
	defer CleanupSignalHandling(sigChan)
	fmt.Println("Starting message consumption. Press Ctrl+C to stop...")

	scanner, err := NewWALScanner(ctx, p.WALName, p.PChannel, p.MQAddr)
	if err != nil {
		return err
	}
	defer scanner.Scanner.Close()

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
		case msg, ok := <-scanner.MessageChan:
			if !ok {
				return errors.New("scanner closed")
			}
			if msg.MessageType().IsSelfControlled() {
				continue
			}
			fmt.Printf("📥%s\n", FormatMessageInfo(msg))
			count++
			if count >= limit {
				fmt.Printf("Reached limit %d, stopping consumption.\n", limit)
				return nil
			}
		}
	}
}
