package common

import (
	"context"
	"path"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

// VChannelMetaKey returns the etcd key holding a vchannel's recovery metadata.
func VChannelMetaKey(basePath string, vchannel string) string {
	pchannel := funcutil.ToPhysicalChannel(vchannel)
	return path.Join(basePath, walRecoveryStoragePrefix, pchannel, walRecoveryStorageDirectoryVChannel, vchannel)
}

// SaveVChannelMeta writes a vchannel's recovery metadata. It refuses to overwrite an
// existing entry: this is only meant to restore a vchannel that is missing, and an entry
// that is already there is the streaming node's own live state.
func SaveVChannelMeta(ctx context.Context, cli kv.MetaKV, basePath string, meta *streamingpb.VChannelMeta) error {
	key := VChannelMetaKey(basePath, meta.GetVchannel())
	if v, err := cli.Load(ctx, key); err == nil && v != "" {
		return errors.Errorf("vchannel meta already exists at %s, refusing to overwrite", key)
	}

	bs, err := proto.Marshal(meta)
	if err != nil {
		return errors.Wrapf(err, "marshal vchannel meta %s", meta.GetVchannel())
	}
	return cli.Save(ctx, key, string(bs))
}
