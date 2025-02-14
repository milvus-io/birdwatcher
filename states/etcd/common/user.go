package common

import (
	"context"
	"path"

	"github.com/samber/lo"

	"github.com/milvus-io/birdwatcher/models"
	internalpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/internalpb"
	"github.com/milvus-io/birdwatcher/states/kv"
)

const (
	userPrefix = `root-coord/credential/users`
)

func ListUsers(ctx context.Context, cli kv.MetaKV, basePath string) ([]*models.UserInfo, error) {
	prefix := path.Join(basePath, userPrefix)

	infos, keys, err := ListJSONObjects[internalpbv2.CredentialInfo](ctx, cli, prefix)
	if err != nil {
		return nil, err
	}

	return lo.Map(infos, func(info *internalpbv2.CredentialInfo, idx int) *models.UserInfo {
		return models.NewUserInfo(info, keys[idx])
	}), nil
}
