package states

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/milvus-io/birdwatcher/eventlog"
	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/mgrpc"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
)

type ListMetricsPortParam struct {
	framework.ParamBase `use:"list metrics-port" desc:"list metrics port for online components"`
	DialTimeout         int64 `name:"dialTimeout" default:"2" desc:"grpc dial timeout in seconds"`
}

// ListMetricsPortCommand returns command logic listing metrics port for all online components.
func (s *InstanceState) ListMetricsPortCommand(ctx context.Context, p *ListMetricsPortParam) error {
	sessions, err := common.ListSessions(ctx, s.client, s.basePath)
	if err != nil {
		return errors.Wrap(err, "failed to list sessions")
	}

	for _, session := range sessions {
		opts := []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
		}

		var conn *grpc.ClientConn
		var err error
		func() {
			dialCtx, cancel := context.WithTimeout(ctx, time.Second*2)
			defer cancel()
			conn, err = grpc.DialContext(dialCtx, session.Address, opts...)
		}()
		if err != nil {
			fmt.Printf("failed to connect to Server(%d) addr: %s, err: %s\n", session.ServerID, session.Address, err.Error())
			continue
		}

		source := getConfigurationSource(session, conn)
		if source == nil {
			continue
		}
		items, _ := mgrpc.GetConfiguration(ctx, source, session.ServerID)
		for _, item := range items {
			if item.GetKey() == "commonmetricsport" {
				fmt.Println(session.ServerName, session.IP(), item.GetValue())
			}
		}
	}

	return nil
}

type ListenEventParam struct {
	framework.ParamBase `use:"listen-events"`
	Localhost           bool `name:"localhost" default:"false" desc:"localhost components"`
}

// ListenEventsCommand returns command logic listen events from grpc event logger.
func (s *InstanceState) ListenEventsCommand(ctx context.Context, p *ListenEventParam) error {
	listeners, err := s.prepareListenerClients(ctx)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	var mut sync.Mutex
	wg.Add(len(listeners))

	for _, listener := range listeners {
		go func(listener *eventlog.Listener) {
			defer wg.Done()
			ch, err := listener.Start(ctx)
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			for evt := range ch {
				// screen.printEvent(event)
				mut.Lock()
				lvl := evt.GetLevel()
				fmt.Printf("[%s][%s]%s\n", time.Unix(0, evt.GetTs()).Format("01/02 15:04:05"), levelColor[lvl].Sprint(lvl.String()), string(evt.Data))
				mut.Unlock()
			}
		}(listener)
	}

	// block until cancel
	<-ctx.Done()
	wg.Wait()
	return nil
}

type portResp struct {
	Status int `json:"status"`
	Port   int `json:"port"`
}

func getEventLogPort(ctx context.Context, ip string, metricPort string) int {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://%s:%s/eventlog", ip, metricPort), nil)
	if err != nil {
		return -1
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return -1
	}
	bs, err := io.ReadAll(resp.Body)
	if err != nil {
		return -1
	}
	r := portResp{}
	json.Unmarshal(bs, &r)
	if r.Status != http.StatusOK {
		return -1
	}
	return r.Port
}

func (s *InstanceState) prepareListenerClients(ctx context.Context) ([]*eventlog.Listener, error) {
	sessions, err := common.ListSessions(ctx, s.client, s.basePath)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list sessions")
	}

	var m sync.Map
	var wg sync.WaitGroup

	sessions = lo.UniqBy[*models.Session, int64](sessions, func(session *models.Session) int64 {
		return session.ServerID
	})
	wg.Add(len(sessions))
	for _, session := range sessions {
		go func(session *models.Session) {
			defer wg.Done()
			opts := []grpc.DialOption{
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithBlock(),
			}

			conn, err := grpc.DialContext(ctx, session.Address, opts...)
			if err != nil {
				fmt.Printf("failed to connect to Server(%d) addr: %s, err: %s\n", session.ServerID, session.Address, err.Error())
				return
			}

			// create configuration source
			source := getConfigurationSource(session, conn)
			if source == nil {
				return
			}

			// fetch configuration items from source
			items, err := mgrpc.GetConfiguration(ctx, source, session.ServerID)
			if err != nil {
				return
			}

			items = lo.Filter(items, func(kv *commonpb.KeyValuePair, _ int) bool {
				return kv.GetKey() == "commonmetricsport"
			})

			if len(items) != 1 {
				return
			}

			item := items[0]
			ip := session.IP()
			port := getEventLogPort(ctx, ip, item.GetValue())
			if port == -1 {
				return
			}
			addr := fmt.Sprintf("%s:%d", ip, port)

			listener, err := eventlog.NewListener(ctx, addr)
			if err != nil {
				return
			}
			m.Store(addr, listener)
		}(session)
	}

	wg.Wait()

	mSize := 0
	m.Range(func(_, _ any) bool {
		mSize++
		return true
	})

	if mSize != len(sessions) {
		return nil, fmt.Errorf("failed to create listener, expected %d, got %d", len(sessions), mSize)
	}

	var listeners []*eventlog.Listener
	m.Range(func(key, value any) bool {
		listener := value.(*eventlog.Listener)
		listeners = append(listeners, listener)
		return true
	})

	return listeners, nil
}

func getConfigurationSource(session *models.Session, conn *grpc.ClientConn) mgrpc.ConfigurationSource {
	var client mgrpc.ConfigurationSource
	switch session.ServerName {
	case "datacoord":
		client = datapb.NewDataCoordClient(conn)
	case "datanode":
		client = datapb.NewDataNodeClient(conn)
	case "indexcoord":
		client = indexpb.NewIndexCoordClient(conn)
	// case "indexnode":
	// 	client = indexpbv2.NewIndexNodeClient(conn)
	case "querycoord":
		client = querypb.NewQueryCoordClient(conn)
	case "querynode":
		client = querypb.NewQueryNodeClient(conn)
	case "rootcoord":
		client = rootcoordpb.NewRootCoordClient(conn)
		//	case "proxy":
		// client:= milvuspb.NewMilvusServiceClient(conn)
		// state.SetNext(getProxy)
	case "milvus":
	}
	return client
}
