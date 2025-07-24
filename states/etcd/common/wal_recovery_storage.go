package common

import (
	"context"
	"encoding/base64"
	"fmt"
	"path"
	"strconv"
	"strings"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/wp"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
)

const (
	walRecoveryStoragePrefix                 = "streamingnode-meta/wal/"
	walRecoveryStorageDirectorySegmentAssign = "segment-assign"
	walRecoveryStorageDirectoryVChannel      = "vchannel"
	walRecoveryStorageDirectorySchema        = "schema"
	walRecoveryStorageConsumeCheckpoint      = "consume-checkpoint"
)

var walNameUnmarshaler map[string]func(string) (string, error)

func init() {
	// TODO: should use milvus/pkg/v2, but there's some package dependency issue, should be fixed later.
	walNameUnmarshaler = make(map[string]func(string) (string, error))
	walNameUnmarshaler["pulsar"] = func(b string) (string, error) {
		bytes, err := base64.StdEncoding.DecodeString(b)
		if err != nil {
			return "", err
		}
		msgID, err := pulsar.DeserializeMessageID(bytes)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%d/%d/%d", msgID.LedgerID(), msgID.EntryID(), msgID.BatchIdx()), nil
	}
	walNameUnmarshaler["kafka"] = func(b string) (string, error) {
		msgID, err := message.DecodeUint64(b)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%d", msgID), nil
	}
	walNameUnmarshaler["rmq"] = func(b string) (string, error) {
		msgID, err := message.DecodeUint64(b)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%d", msgID), nil
	}
	walNameUnmarshaler["wp"] = func(b string) (string, error) {
		bytes, err := base64.StdEncoding.DecodeString(b)
		if err != nil {
			return "", err
		}
		msgID, err := wp.UnmarshalMessageID(string(bytes))
		if err != nil {
			return "", err
		}
		return msgID.String(), nil
	}
}

// RecoveryStorageMeta is the meta data of recovery storage
type RecoveryStorageMeta struct {
	Channel           *streamingpb.PChannelMeta
	Checkpoints       *streamingpb.WALCheckpoint
	VChannels         []*streamingpb.VChannelMeta
	Segments          map[string][]*streamingpb.SegmentAssignmentMeta
	RedundantSchemas  map[string][]*streamingpb.CollectionSchemaOfVChannel
	RedundantSegments map[string][]*streamingpb.SegmentAssignmentMeta
}

// FormatSegmentAssignmentMeta format the segment assignment meta.
func FormatSegmentAssignmentMeta(segment *streamingpb.SegmentAssignmentMeta) string {
	return fmt.Sprintf("%d@%d[%d,%d]", segment.SegmentId, segment.CheckpointTimeTick, segment.Stat.InsertedRows, segment.Stat.InsertedBinarySize)
}

// FormatSchema format the schema.
func FormatSchema(schema *streamingpb.CollectionSchemaOfVChannel) string {
	return fmt.Sprintf("schema@%d", schema.CheckpointTimeTick)
}

// FormatWALCheckpoint format the wal checkpoint.
func FormatWALCheckpoint(walName string, checkpoint *streamingpb.WALCheckpoint) string {
	id := GetMessageIDString(walName, checkpoint.MessageId.Id)
	return fmt.Sprintf("%s@%d", id, checkpoint.TimeTick)
}

// GetMessageIDString get the message id string.
func GetMessageIDString(walName string, id string) string {
	if walName == "" {
		return GuessWALName(id)
	}

	messageIDString, err := walNameUnmarshaler[walName](id)
	if err != nil {
		return fmt.Sprintf("%s[unknown]", id)
	}
	return messageIDString
}

// GuessWALName guess the wal name from the message id.
func GuessWALName(id string) string {
	for _, walName := range []string{"pulsar", "kafka", "rmq", "wp"} {
		messageIDString, err := walNameUnmarshaler[walName](id)
		if err != nil {
			continue
		}
		return messageIDString
	}
	return fmt.Sprintf("%s[unknown]", id)
}

// ListWALRecoveryStorage list the wal recovery storage meta data
func ListWALRecoveryStorage(ctx context.Context, cli kv.MetaKV, basePath string, pchannel string) (*RecoveryStorageMeta, error) {
	channel, err := ListWALDistribution(ctx, cli, basePath, pchannel)
	if err != nil {
		return nil, err
	}
	if len(channel) == 0 {
		return nil, errors.Errorf("channel not found for %s", pchannel)
	}

	prefix := path.Join(basePath, walRecoveryStoragePrefix, pchannel) + "/"

	keys, vals, err := cli.LoadWithPrefix(ctx, prefix)
	if err != nil {
		return nil, err
	}
	keys = removePrefix(keys, prefix)

	var checkpoint *streamingpb.WALCheckpoint
	vchannels := make([]*streamingpb.VChannelMeta, 0)
	schemas := make(map[string][]*streamingpb.CollectionSchemaOfVChannel, 0)
	segments := make(map[string][]*streamingpb.SegmentAssignmentMeta, 0)
	for idx, key := range keys {
		ks := strings.Split(key, "/")
		switch ks[0] {
		case walRecoveryStorageConsumeCheckpoint:
			cp := &streamingpb.WALCheckpoint{}
			if err := proto.Unmarshal([]byte(vals[idx]), cp); err != nil {
				return nil, errors.Errorf("fail to unmarshal checkpoint at %s: %w", key, err)
			}
			checkpoint = cp
			continue
		case walRecoveryStorageDirectorySegmentAssign:
			// ks[0] is segment id
			segment := &streamingpb.SegmentAssignmentMeta{}
			if err := proto.Unmarshal([]byte(vals[idx]), segment); err != nil {
				return nil, errors.Errorf("fail to unmarshal segment assignment meta at %s: %w", key, err)
			}
			segments[segment.Vchannel] = append(segments[segment.Vchannel], segment)
			continue
		case walRecoveryStorageDirectoryVChannel:
			ks = ks[1:]
			// ks[0] is vchannel name
			if len(ks) == 1 {
				vchannel := &streamingpb.VChannelMeta{}
				if err := proto.Unmarshal([]byte(vals[idx]), vchannel); err != nil {
					return nil, errors.Errorf("fail to unmarshal vchannel meta at %s: %w", key, err)
				}
				vchannels = append(vchannels, vchannel)
				continue
			} else if len(ks) == 3 && ks[1] == walRecoveryStorageDirectorySchema {
				schemas[ks[2]] = make([]*streamingpb.CollectionSchemaOfVChannel, 0)
				schema := &streamingpb.CollectionSchemaOfVChannel{}
				if err := proto.Unmarshal([]byte(vals[idx]), schema); err != nil {
					return nil, errors.Errorf("fail to unmarshal schema at %s: %w", key, err)
				}
				schemas[ks[0]] = append(schemas[ks[0]], schema)
				continue
			}
		}
		log.Error("error: unknown vchannel meta", zap.String("key", key))
	}

	segmentsGroupByVChannel := make(map[string][]*streamingpb.SegmentAssignmentMeta)
	for _, vchannel := range vchannels {
		if _, ok := schemas[vchannel.Vchannel]; ok {
			vchannel.CollectionInfo.Schemas = schemas[vchannel.Vchannel]
			delete(schemas, vchannel.Vchannel)
		}
		if _, ok := segments[vchannel.Vchannel]; ok {
			segmentsGroupByVChannel[vchannel.Vchannel] = segments[vchannel.Vchannel]
			delete(segments, vchannel.Vchannel)
		}
	}
	return &RecoveryStorageMeta{
		Channel:           channel[0],
		Checkpoints:       checkpoint,
		VChannels:         vchannels,
		Segments:          segmentsGroupByVChannel,
		RedundantSchemas:  schemas,
		RedundantSegments: segments,
	}, nil
}

// SaveSchemaForVChannel save the schema for vchannel
func SaveSchemaForVChannel(ctx context.Context, cli kv.MetaKV, basePath string, vchannel *streamingpb.VChannelMeta, schema *streamingpb.CollectionSchemaOfVChannel) error {
	pchannel := funcutil.ToPhysicalChannel(vchannel.Vchannel)
	prefix := path.Join(basePath, walRecoveryStoragePrefix, pchannel, walRecoveryStorageDirectoryVChannel, vchannel.Vchannel, walRecoveryStorageDirectorySchema)
	keys, _, err := cli.LoadWithPrefix(ctx, prefix)
	if err != nil {
		return err
	}
	if len(keys) > 0 {
		return errors.Errorf("schema already exists for vchannel %s, key: %s, please remove it first", vchannel.Vchannel, keys[0])
	}

	schemaPath := path.Join(basePath, walRecoveryStoragePrefix, pchannel, walRecoveryStorageDirectoryVChannel, vchannel.Vchannel, walRecoveryStorageDirectorySchema, strconv.FormatUint(schema.CheckpointTimeTick, 10))
	schemaBytes, err := proto.Marshal(schema)
	if err != nil {
		return err
	}
	return cli.Save(ctx, schemaPath, string(schemaBytes))
}

func removePrefix(keys []string, prefix string) []string {
	for i, key := range keys {
		keys[i] = strings.TrimPrefix(key, prefix)
	}
	return keys
}
