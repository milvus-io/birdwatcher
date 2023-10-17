package models

import (
	"path"

	internalpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/internalpb"
)

type UserInfo struct {
	Username          string
	EncryptedPassword string
	Tenant            string
	IsSuper           bool
	Sha256Password    string
}

func NewUserInfo(info *internalpbv2.CredentialInfo, key string) *UserInfo {
	return &UserInfo{
		Username:          path.Base(key),
		EncryptedPassword: info.GetEncryptedPassword(),
		Tenant:            info.GetTenant(),
		IsSuper:           info.GetIsSuper(),
		Sha256Password:    info.GetSha256Password(),
	}
}
