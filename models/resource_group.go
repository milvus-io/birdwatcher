package models

import (
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
)

type ResourceGroup = ProtoWrapper[*querypb.ResourceGroup]

func NewResourceGroup(rg *querypb.ResourceGroup, key string) *ResourceGroup {
	return NewProtoWrapper(rg, key)
}
