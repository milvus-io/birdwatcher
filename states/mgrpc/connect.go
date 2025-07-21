package mgrpc

import (
	"context"
	"strings"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	metakv "github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
)

type SessionClient[T any] struct {
	Session *models.Session
	Client  T
}

// SessionMatch is the util func handles mixcoord & XXXXcoord match logic
func SessionMatch(session *models.Session, sessionType string) bool {
	if session.ServerName == sessionType {
		return true
	}
	if strings.HasSuffix(sessionType, "coord") {
		return session.ServerName == "mixcoord"
	}
	return false
}

func GetSessionConnection(ctx context.Context, client metakv.MetaKV, basePath string, sessionType string, id int64) (sessions []*models.Session, conns []*grpc.ClientConn, err error) {
	allSessions, err := common.ListSessions(ctx, client, basePath)
	if err != nil {
		return nil, nil, err
	}
	for _, session := range allSessions {
		if SessionMatch(session, sessionType) {
			if id != 0 && session.ServerID != id {
				continue
			}
			opts := []grpc.DialOption{
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithBlock(),
			}
			conn, err := grpc.DialContext(ctx, session.Address, opts...)
			if err != nil {
				return nil, nil, err
			}

			sessions = append(sessions, session)
			conns = append(conns, conn)
		}
	}
	if len(sessions) == 0 {
		return nil, nil, errors.Newf("session of %s not found", sessionType)
	}
	return sessions, conns, nil
}

func ConnectRootCoord(ctx context.Context, client metakv.MetaKV, basePath string, id int64) (*SessionClient[rootcoordpb.RootCoordClient], error) {
	sessions, conns, err := GetSessionConnection(ctx, client, basePath, "rootcoord", id)
	if err != nil {
		return nil, err
	}
	if len(sessions) == 0 {
		return nil, errors.New("rootcoord session not found")
	}
	session := sessions[0]
	conn := conns[0]

	return &SessionClient[rootcoordpb.RootCoordClient]{
		Session: session,
		Client:  rootcoordpb.NewRootCoordClient(conn),
	}, nil
}

func ConnectQueryNodes(ctx context.Context, client metakv.MetaKV, basePath string, id int64) ([]*SessionClient[querypb.QueryNodeClient], error) {
	sessions, conns, err := GetSessionConnection(ctx, client, basePath, "querynode", id)
	if err != nil {
		return nil, err
	}
	if len(sessions) == 0 {
		return nil, errors.New("querynode session not found")
	}
	var clients []*SessionClient[querypb.QueryNodeClient]
	for idx, session := range sessions {
		conn := conns[idx]
		clients = append(clients, &SessionClient[querypb.QueryNodeClient]{
			Session: session,
			Client:  querypb.NewQueryNodeClient(conn),
		})
	}
	return clients, nil
}
