package common

import (
	"context"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/kv"
)

func ListServers(ctx context.Context, cli kv.MetaKV, basePath string, serverName string) ([]*models.Session, error) {
	sessions, err := ListSessions(ctx, cli, basePath)
	if err != nil {
		return nil, err
	}
	targetSessions := make([]*models.Session, 0)
	for _, session := range sessions {
		if session.ServerName == serverName {
			targetSessions = append(targetSessions, session)
		}
	}
	return targetSessions, nil
}
