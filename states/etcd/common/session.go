package common

import (
	"context"
	"encoding/json"
	"path"
	"time"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/kv"
)

const (
	sessionPrefix = `session`
)

// ListSessions returns all session.
func ListSessions(cli kv.MetaKV, basePath string) ([]*models.Session, error) {
	prefix := path.Join(basePath, sessionPrefix)
	return ListSessionsByPrefix(cli, prefix)
}

// ListSessionsByPrefix returns all session with provided prefix.
func ListSessionsByPrefix(cli kv.MetaKV, prefix string) ([]*models.Session, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	keys, vals, err := cli.LoadWithPrefix(ctx, prefix)
	if err != nil {
		return nil, err
	}

	sessions := make([]*models.Session, 0, len(vals))
	for idx, val := range vals {
		session := &models.Session{}
		err := json.Unmarshal([]byte(val), session)
		if err != nil {
			continue
		}
		session.SetKey(keys[idx])

		sessions = append(sessions, session)
	}
	return sessions, nil
}
