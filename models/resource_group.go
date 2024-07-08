package models

import (
	querypbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/querypb"
)

type ResourceGroup struct {
	querypbv2.ResourceGroup
	key string
}

func NewResourceGroup(rg querypbv2.ResourceGroup, key string) *ResourceGroup {
	return &ResourceGroup{
		ResourceGroup: rg,
		key:           key,
	}
}
