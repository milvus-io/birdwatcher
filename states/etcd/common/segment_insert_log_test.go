package common

import (
	"context"
	"fmt"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func marshalFieldBinlog(t *testing.T, fieldID int64, logIDs ...int64) string {
	t.Helper()
	fbl := &datapb.FieldBinlog{FieldID: fieldID}
	for _, id := range logIDs {
		fbl.Binlogs = append(fbl.Binlogs, &datapb.Binlog{LogID: id, EntriesNum: 100})
	}
	bs, err := proto.Marshal(fbl)
	require.NoError(t, err)
	return string(bs)
}

func TestRemoveSegmentInsertLogPath(t *testing.T) {
	const (
		basePath     = "by-dev/meta"
		collectionID = 451336372851573427
		partitionID  = 451336372851573428
		segmentID    = 451336372851573996
		fieldID      = 1
	)
	// Insert binlogs live under datacoord-meta/binlog/<coll>/<part>/<seg>/<field>/<group>,
	// one FieldBinlog value per key holding the list of binlog entries.
	binlogKey := path.Join(basePath, "datacoord-meta", "binlog",
		fmt.Sprintf("%d/%d/%d", collectionID, partitionID, segmentID),
		fmt.Sprintf("%d/%d", fieldID, 1))

	t.Run("removes the missing logID and keeps the rest", func(t *testing.T) {
		cli := &segmentListKV{data: map[string]string{
			binlogKey: marshalFieldBinlog(t, fieldID, 100, 200, 300),
		}}

		err := RemoveSegmentInsertLogPath(context.Background(), cli, basePath, collectionID, partitionID, segmentID, fieldID, 200)
		require.NoError(t, err)

		raw, ok := cli.data[binlogKey]
		require.True(t, ok, "key with remaining binlogs must be kept")
		fbl := &datapb.FieldBinlog{}
		require.NoError(t, proto.Unmarshal([]byte(raw), fbl))
		require.Len(t, fbl.Binlogs, 2)
		require.Equal(t, int64(100), fbl.Binlogs[0].LogID)
		require.Equal(t, int64(300), fbl.Binlogs[1].LogID)
	})

	t.Run("deletes the key when the field's binlog list becomes empty", func(t *testing.T) {
		cli := &segmentListKV{data: map[string]string{
			binlogKey: marshalFieldBinlog(t, fieldID, 200),
		}}

		err := RemoveSegmentInsertLogPath(context.Background(), cli, basePath, collectionID, partitionID, segmentID, fieldID, 200)
		require.NoError(t, err)
		_, ok := cli.data[binlogKey]
		require.False(t, ok, "key must be removed when its only binlog is gone")
	})

	t.Run("returns an error when the logID is absent", func(t *testing.T) {
		cli := &segmentListKV{data: map[string]string{
			binlogKey: marshalFieldBinlog(t, fieldID, 100, 200),
		}}

		err := RemoveSegmentInsertLogPath(context.Background(), cli, basePath, collectionID, partitionID, segmentID, fieldID, 999)
		require.Error(t, err, "a logID that is not present must be reported, not silently ignored")

		raw, ok := cli.data[binlogKey]
		require.True(t, ok)
		fbl := &datapb.FieldBinlog{}
		require.NoError(t, proto.Unmarshal([]byte(raw), fbl))
		require.Len(t, fbl.Binlogs, 2, "existing binlogs must be untouched when the target is absent")
	})
}
