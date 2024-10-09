package common

import (
	"context"
	"encoding/json"
	"path"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/birdwatcher/models"
)

const (
	sessionPrefix = `session`
)

// ListSessions returns all session.
func ListSessions(ctx context.Context, cli clientv3.KV, basePath string) ([]*models.Session, error) {
	prefix := path.Join(basePath, sessionPrefix)
	return ListSessionsByPrefix(ctx, cli, prefix)
}

// ListSessionsByPrefix returns all session with provided prefix.
func ListSessionsByPrefix(ctx context.Context, cli clientv3.KV, prefix string) ([]*models.Session, error) {
	resp, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	sessions := make([]*models.Session, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		session := &models.Session{}
		err := json.Unmarshal(kv.Value, session)
		if err != nil {
			continue
		}
		session.SetKey(string(kv.Key))

		sessions = append(sessions, session)
	}
	return sessions, nil
}
