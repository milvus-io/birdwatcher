package models

import (
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
)

// CollectionLoaded models collection loaded information.
type CollectionLoaded = ProtoWrapper[*querypb.CollectionLoadInfo]

func NewCollectionLoaded(info *querypb.CollectionLoadInfo, key string) *CollectionLoaded {
	return NewProtoWrapper(info, key)
}
