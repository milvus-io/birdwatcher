package repair

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

type saveRecordingKV struct {
	kv.MetaKV
	saveKeys   []string
	saveValues []string
}

func (k *saveRecordingKV) Save(ctx context.Context, key, value string) error {
	k.saveKeys = append(k.saveKeys, key)
	k.saveValues = append(k.saveValues, value)
	return nil
}

func TestWriteRepairedIndex(t *testing.T) {
	t.Run("marshal failure does not write to etcd", func(t *testing.T) {
		cli := &saveRecordingKV{}
		index := &indexpb.FieldIndex{
			IndexInfo: &indexpb.IndexInfo{
				CollectionID: 100,
				IndexID:      200,
				// invalid UTF-8 makes proto.Marshal fail.
				IndexName: "\xff\xfe\x00\x80invalid",
			},
		}

		err := writeRepairedIndex(cli, "by-dev/meta", index)

		require.Error(t, err)
		assert.Empty(t, cli.saveKeys, "Save must not be called when marshaling fails")
	})

	t.Run("valid index is saved", func(t *testing.T) {
		cli := &saveRecordingKV{}
		index := &indexpb.FieldIndex{
			IndexInfo: &indexpb.IndexInfo{
				CollectionID: 100,
				IndexID:      200,
				IndexName:    "index_name",
			},
		}

		err := writeRepairedIndex(cli, "by-dev/meta", index)

		require.NoError(t, err)
		require.Len(t, cli.saveKeys, 1)
		assert.Equal(t, "by-dev/meta/field-index/100/200", cli.saveKeys[0])

		saved := &indexpb.FieldIndex{}
		require.NoError(t, proto.Unmarshal([]byte(cli.saveValues[0]), saved))
		assert.Equal(t, index.GetIndexInfo().GetIndexName(), saved.GetIndexInfo().GetIndexName())
	})
}
