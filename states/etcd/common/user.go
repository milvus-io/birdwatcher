package common

import (
	"context"
	"path"

	"github.com/milvus-io/birdwatcher/models"
	internalpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/internalpb"
	"github.com/samber/lo"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	userPrefix = `root-coord/credential/users`
)

func ListUsers(ctx context.Context, cli clientv3.KV, basePath string) ([]*models.UserInfo, error) {
	prefix := path.Join(basePath, userPrefix)

	infos, keys, err := ListJSONObjects[internalpbv2.CredentialInfo](ctx, cli, prefix)

	if err != nil {
		return nil, err
	}

	return lo.Map(infos, func(info *internalpbv2.CredentialInfo, idx int) *models.UserInfo {
		return models.NewUserInfo(info, keys[idx])
	}), nil
}
