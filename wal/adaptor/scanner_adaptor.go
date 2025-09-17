package adaptor

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/birdwatcher/wal/utility"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/helper"
	_ "github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/pulsar"
)

type ChanMessageHandler chan message.ImmutableMessage

func (h ChanMessageHandler) Handle(param message.HandleParam) message.HandleResult {
	var sendingCh chan message.ImmutableMessage
	if param.Message != nil {
		sendingCh = h
	}
	select {
	case <-param.Ctx.Done():
		return message.HandleResult{Error: param.Ctx.Err()}
	case msg, ok := <-param.Upstream:
		if !ok {
			panic("unreachable code: upstream should never closed")
		}
		return message.HandleResult{Incoming: msg}
	case sendingCh <- param.Message:
		return message.HandleResult{MessageHandled: true}
	}
}

func (d ChanMessageHandler) Close() {
	close(d)
}

// newScannerAdaptor creates a new scanner adaptor.
func newScannerAdaptor(
	name string,
	l walimpls.ROWALImpls,
	readOption ReadOption,
	cleanup func(),
) *scannerAdaptorImpl {
	if readOption.MesasgeHandler == nil {
		readOption.MesasgeHandler = ChanMessageHandler(make(chan message.ImmutableMessage))
	}
	options.GetFilterFunc(readOption.MessageFilter)
	logger := log.With(
		log.FieldComponent("scanner"),
		zap.String("name", name),
		zap.String("channel", l.Channel().Name),
	)
	s := &scannerAdaptorImpl{
		logger:        logger,
		recovery:      false,
		innerWAL:      l,
		readOption:    readOption,
		filterFunc:    options.GetFilterFunc(readOption.MessageFilter),
		reorderBuffer: utility.NewReOrderBuffer(),
		pendingQueue:  utility.NewPendingQueue(),
		txnBuffer:     utility.NewTxnBuffer(logger),
		cleanup:       cleanup,
		ScannerHelper: helper.NewScannerHelper(name),
	}
	go s.execute()
	return s
}

// scannerAdaptorImpl is a wrapper of ScannerImpls to extend it into a Scanner interface.
type scannerAdaptorImpl struct {
	*helper.ScannerHelper
	recovery      bool
	logger        *log.MLogger
	innerWAL      walimpls.ROWALImpls
	readOption    ReadOption
	filterFunc    func(message.ImmutableMessage) bool
	reorderBuffer *utility.ReOrderByTimeTickBuffer // support time tick reorder.
	pendingQueue  *utility.PendingQueue
	txnBuffer     *utility.TxnBuffer // txn buffer for txn message.

	cleanup   func()
	clearOnce sync.Once
}

// Channel returns the channel assignment info of the wal.
func (s *scannerAdaptorImpl) Channel() types.PChannelInfo {
	return s.innerWAL.Channel()
}

// Chan returns the message channel of the scanner.
func (s *scannerAdaptorImpl) Chan() <-chan message.ImmutableMessage {
	return s.readOption.MesasgeHandler.(ChanMessageHandler)
}

// Close the scanner, release the underlying resources.
// Return the error same with `Error`
func (s *scannerAdaptorImpl) Close() error {
	err := s.ScannerHelper.Close()
	// Close may be called multiple times, so we need to clear the resources only once.
	s.clear()
	return err
}

// clear clears the resources of the scanner.
func (s *scannerAdaptorImpl) clear() {
	s.clearOnce.Do(func() {
		if s.cleanup != nil {
			s.cleanup()
		}
	})
}

func (s *scannerAdaptorImpl) execute() {
	defer func() {
		s.readOption.MesasgeHandler.Close()
		s.Finish(nil)
		s.logger.Info("scanner is closed")
	}()
	s.logger.Info("scanner start background task")

	msgChan := make(chan message.ImmutableMessage)

	ch := make(chan struct{})
	defer func() { <-ch }()
	// TODO: optimize the extra goroutine here after msgstream is removed.
	go func() {
		defer close(ch)
		err := s.produceEventLoop(msgChan)
		if errors.Is(err, context.Canceled) {
			s.logger.Info("the produce event loop of scanner is closed")
			return
		}
		s.logger.Warn("the produce event loop of scanner is closed with unexpected error", zap.Error(err))
	}()

	err := s.consumeEventLoop(msgChan)
	if errors.Is(err, context.Canceled) {
		s.logger.Info("the consuming event loop of scanner is closed")
		return
	}
	s.logger.Warn("the consuming event loop of scanner is closed with unexpected error", zap.Error(err))
}

// produceEventLoop produces the message from the wal and write ahead buffer.
func (s *scannerAdaptorImpl) produceEventLoop(msgChan chan<- message.ImmutableMessage) error {
	scanner := newCatchupScanner(s.Name(), s.logger, s.innerWAL, s.readOption.DeliverPolicy, msgChan)
	s.logger.Info("start produce loop of scanner")

	return scanner.Do(s.Context())
}

// consumeEventLoop consumes the message from the message channel and handle it.
func (s *scannerAdaptorImpl) consumeEventLoop(msgChan <-chan message.ImmutableMessage) error {
	for {
		var upstream <-chan message.ImmutableMessage
		if s.pendingQueue.Len() > 16 {
			// If the pending queue is full, we need to wait until it's consumed to avoid scanner overloading.
			upstream = nil
		} else {
			upstream = msgChan
		}
		// generate the event channel and do the event loop.
		handleResult := s.readOption.MesasgeHandler.Handle(message.HandleParam{
			Ctx:      s.Context(),
			Upstream: upstream,
			Message:  s.pendingQueue.Next(),
		})
		if handleResult.Error != nil {
			return handleResult.Error
		}
		if handleResult.MessageHandled {
			s.pendingQueue.UnsafeAdvance()
		}
		if handleResult.Incoming != nil {
			s.handleUpstream(handleResult.Incoming)
		}
	}
}

// handleUpstream handles the incoming message from the upstream.
func (s *scannerAdaptorImpl) handleUpstream(msg message.ImmutableMessage) {
	// Filtering the message if needed.
	// System message should never be filtered.
	if s.filterFunc != nil && !s.filterFunc(msg) {
		return
	}

	// Observe the message.
	isTailing := false
	if msg.MessageType() == message.MessageTypeTimeTick {
		// If the time tick message incoming,
		// the reorder buffer can be consumed until latest confirmed timetick.
		messages := s.reorderBuffer.PopUtilTimeTick(msg.TimeTick())

		// There's some txn message need to hold until confirmed, so we need to handle them in txn buffer.
		msgs := s.txnBuffer.HandleImmutableMessages(messages, msg.TimeTick())

		if len(msgs) > 0 {
			// Push the confirmed messages into pending queue for consuming.
			s.pendingQueue.Add(msgs)
		}
		if msg.IsPersisted() || s.pendingQueue.Len() == 0 {
			// If the ts message is persisted, it must can be seen by the consumer.
			//
			// Otherwise if there's no new message incoming and there's no pending message in the queue.
			// Add current timetick message into pending queue to make timetick push forward.
			// TODO: current milvus can only run on timetick pushing,
			// after qview is applied, those trival time tick message can be erased.
			s.pendingQueue.Add([]message.ImmutableMessage{msg})
		}
		return
	}

	// Filtering the vchannel
	// If the message is not belong to any vchannel, it should be broadcasted to all vchannels.
	// Otherwise, it should be filtered by vchannel.
	if msg.VChannel() != "" && s.readOption.VChannel != "" && s.readOption.VChannel != msg.VChannel() {
		return
	}
	// otherwise add message into reorder buffer directly.
	if err := s.reorderBuffer.Push(msg); err != nil {
		if errors.Is(err, utility.ErrTimeTickVoilation) {
		}
		s.logger.Warn("failed to push message into reorder buffer",
			log.FieldMessage(msg),
			zap.Bool("tailing", isTailing),
			zap.Error(err))
	}
	// Observe the filtered message.
	if s.logger.Level().Enabled(zap.DebugLevel) {
		// Log the message if the log level is debug.
		s.logger.Debug("push message into reorder buffer",
			log.FieldMessage(msg),
			zap.Bool("tailing", isTailing))
	}
}
