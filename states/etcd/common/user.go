package common

import (
	"context"
	"path"

	"github.com/samber/lo"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
)

const (
	userPrefix = `root-coord/credential/users`
)

func ListUsers(ctx context.Context, cli kv.MetaKV, basePath string) ([]*models.UserInfo, error) {
	prefix := path.Join(basePath, userPrefix)

	infos, keys, err := ListJSONObjects[internalpb.CredentialInfo](ctx, cli, prefix)
	if err != nil {
		return nil, err
	}

	return lo.Map(infos, func(info *internalpb.CredentialInfo, idx int) *models.UserInfo {
		return models.NewUserInfo(info, keys[idx])
	}), nil
}
