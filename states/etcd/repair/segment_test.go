package repair

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func TestWriteRepairedSegment(t *testing.T) {
	t.Run("marshal failure does not write to etcd", func(t *testing.T) {
		cli := &saveRecordingKV{}
		segment := &datapb.SegmentInfo{
			ID:           1,
			CollectionID: 100,
			PartitionID:  10,
			// invalid UTF-8 makes proto.Marshal fail.
			InsertChannel: "\xff\xfe\x00\x80invalid",
		}

		err := writeRepairedSegment(cli, "by-dev/meta", segment)

		require.Error(t, err)
		assert.Empty(t, cli.saveKeys, "Save must not be called when marshaling fails")
	})

	t.Run("valid segment is saved", func(t *testing.T) {
		cli := &saveRecordingKV{}
		segment := &datapb.SegmentInfo{
			ID:            1,
			CollectionID:  100,
			PartitionID:   10,
			InsertChannel: "ch-0",
		}

		err := writeRepairedSegment(cli, "by-dev/meta", segment)

		require.NoError(t, err)
		require.Len(t, cli.saveKeys, 1)
		assert.Equal(t, "by-dev/meta/datacoord-meta/s/100/10/1", cli.saveKeys[0])

		saved := &datapb.SegmentInfo{}
		require.NoError(t, proto.Unmarshal([]byte(cli.saveValues[0]), saved))
		assert.Equal(t, segment.GetInsertChannel(), saved.GetInsertChannel())
	})
}
