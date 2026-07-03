package repair

import (
	"context"

	"github.com/milvus-io/birdwatcher/configs"
	"github.com/milvus-io/birdwatcher/oss"
	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/birdwatcher/states/ossutil"
)

type ObjectStoreResolver func(ctx context.Context, params ...oss.MinioConnectParam) (*oss.ResolvedObjectStore, error)

type ComponentRepair struct {
	client              kv.MetaKV
	config              *configs.Config
	basePath            string
	objectStoreResolver ObjectStoreResolver
}

func NewComponent(cli kv.MetaKV, config *configs.Config, basePath string, resolver ...ObjectStoreResolver) *ComponentRepair {
	var objectStoreResolver ObjectStoreResolver
	if len(resolver) > 0 {
		objectStoreResolver = resolver[0]
	}
	return &ComponentRepair{
		client:              cli,
		config:              config,
		basePath:            basePath,
		objectStoreResolver: objectStoreResolver,
	}
}

func (c *ComponentRepair) ObjectStoreResolver() ObjectStoreResolver {
	return c.objectStoreResolver
}

func (c *ComponentRepair) getObjectStore(ctx context.Context, params ...oss.MinioConnectParam) (*oss.ResolvedObjectStore, error) {
	if c.objectStoreResolver != nil {
		resolved, err := c.objectStoreResolver(ctx, params...)
		if err != nil {
			return nil, err
		}
		if resolved != nil {
			return resolved, nil
		}
	}
	return ossutil.GetObjectStoreFromCfg(ctx, c.client, c.basePath, params...)
}
