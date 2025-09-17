package adaptor

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// newCatchupScanner creates a new catchup scanner.
func newCatchupScanner(
	scannerName string,
	logger *log.MLogger,
	innerWAL walimpls.ROWALImpls,
	deliverPolicy options.DeliverPolicy,
	msgChan chan<- message.ImmutableMessage,
) *catchupScanner {
	return &catchupScanner{
		scannerName:   scannerName,
		logger:        logger,
		innerWAL:      innerWAL,
		msgChan:       msgChan,
		deliverPolicy: deliverPolicy,
	}
}

// catchupScanner is a scanner that make a read at underlying wal, and try to catchup the writeahead buffer then switch to tailing mode.
type catchupScanner struct {
	scannerName   string
	logger        *log.MLogger
	innerWAL      walimpls.ROWALImpls
	msgChan       chan<- message.ImmutableMessage
	deliverPolicy options.DeliverPolicy
}

func (s *catchupScanner) Do(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	scanner, err := s.createReaderWithBackoff(ctx, s.deliverPolicy)
	if err != nil {
		// Only the cancellation error will be returned, other error will keep backoff.
		return err
	}
	err = s.consumeWithScanner(ctx, scanner)
	if err != nil {
		s.logger.Warn("scanner consuming was interrpurted with error, start a backoff", zap.Error(err))
		return err
	}
	return nil
}

func (s *catchupScanner) handleMessage(ctx context.Context, msg message.ImmutableMessage) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.msgChan <- msg:
		return nil
	}
}

func (s *catchupScanner) consumeWithScanner(ctx context.Context, scanner walimpls.ScannerImpls) error {
	defer scanner.Close()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-scanner.Chan():
			if !ok {
				return scanner.Error()
			}
			if err := s.handleMessage(ctx, msg); err != nil {
				return err
			}
			if msg.MessageType() != message.MessageTypeTimeTick {
				// Only timetick message is keep the same order with the write ahead buffer.
				// So we can only use the timetick message to catchup the write ahead buffer.
				continue
			}
		}
	}
}

func (s *catchupScanner) createReaderWithBackoff(ctx context.Context, deliverPolicy options.DeliverPolicy) (walimpls.ScannerImpls, error) {
	backoffTimer := typeutil.NewBackoffTimer(typeutil.BackoffTimerConfig{
		Default: 5 * time.Second,
		Backoff: typeutil.BackoffConfig{
			InitialInterval: 100 * time.Millisecond,
			Multiplier:      2.0,
			MaxInterval:     5 * time.Second,
		},
	})
	backoffTimer.EnableBackoff()
	for {
		bufSize := paramtable.Get().StreamingCfg.WALReadAheadBufferLength.GetAsInt()
		if bufSize < 0 {
			bufSize = 0
		}
		innerScanner, err := s.innerWAL.Read(ctx, walimpls.ReadOption{
			Name:                s.scannerName,
			DeliverPolicy:       deliverPolicy,
			ReadAheadBufferSize: bufSize,
		})
		if err == nil {
			return innerScanner, nil
		}
		if ctx.Err() != nil {
			// The scanner is closing, so stop the backoff.
			return nil, ctx.Err()
		}
		waker, nextInterval := backoffTimer.NextTimer()
		s.logger.Warn("create underlying scanner for wal scanner, start a backoff",
			zap.Duration("nextInterval", nextInterval),
			zap.Error(err),
		)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-waker:
		}
	}
}
