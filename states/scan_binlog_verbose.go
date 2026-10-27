package states

import (
	"context"

	"github.com/milvus-io/birdwatcher/oss"
	storagecommon "github.com/milvus-io/birdwatcher/storage/common"
)

func openScanObject(ctx context.Context, store oss.ObjectStore, key string) (storagecommon.ReadSeeker, error) {
	end := storagecommon.ScanOperation(ctx, "object.open path=%q", storagecommon.ScanLogPath(key))
	obj, err := store.Open(ctx, key)
	end(err)
	if err != nil {
		return obj, err
	}
	return storagecommon.WithScanReadLogging(ctx, key, obj), nil
}
