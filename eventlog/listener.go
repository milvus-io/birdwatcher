package eventlog

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Listener struct {
	conn   *grpc.ClientConn
	client EventLogServiceClient
	stream EventLogService_ListenClient
	closed chan struct{}
}

func NewListener(ctx context.Context, addr string) (*Listener, error) {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithTimeout(time.Second),
	}

	conn, err := grpc.DialContext(ctx, addr, opts...)
	if err != nil {
		return nil, err
	}

	client := NewEventLogServiceClient(conn)
	return &Listener{
		conn:   conn,
		client: client,
	}, nil
}

func (l *Listener) Start(ctx context.Context) (<-chan *Event, error) {
	ch := make(chan *Event, 100)
	s, err := l.client.Listen(ctx, &ListenRequest{})
	if err != nil {
		return nil, err
	}

	go func() {
		defer close(ch)
		for {
			evt, err := s.Recv()
			if err != nil {
				return
			}
			select {
			case ch <- evt:
			case <-l.closed:
				return
			}
		}
	}()

	return ch, nil
}

func (l *Listener) Stop() {
	close(l.closed)
	if l.stream != nil {
		l.stream.CloseSend()
	}
}
