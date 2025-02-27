package models

import (
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
)

type UserInfo struct {
	*internalpb.CredentialInfo

	key string
}

func NewUserInfo(info *internalpb.CredentialInfo, key string) *UserInfo {
	return &UserInfo{
		CredentialInfo: info,
		key:            key,
	}
}
