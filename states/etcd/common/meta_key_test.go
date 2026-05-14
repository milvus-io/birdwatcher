package common

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMetaKeySpecScanTargets(t *testing.T) {
	spec := MetaKeySpec{
		Prefix: "datacoord-meta/s",
		Parts:  []MetaKeyPart{KeyCollectionID, KeyPartitionID, KeySegmentID},
	}

	target := spec.BuildScanTarget("root", NewMetaKeyHints().WithInt64(KeyCollectionID, 100))
	require.False(t, target.Exact)
	require.Equal(t, "root/datacoord-meta/s/100/", target.Key)

	target = spec.BuildScanTarget("root", NewMetaKeyHints().WithInt64(KeySegmentID, 300))
	require.False(t, target.Exact)
	require.Equal(t, "root/datacoord-meta/s/", target.Key)

	target = spec.BuildScanTarget("root", NewMetaKeyHints().WithInt64(KeyCollectionID, 100).WithInt64(KeyPartitionID, 200).WithInt64(KeySegmentID, 300))
	require.True(t, target.Exact)
	require.Equal(t, "root/datacoord-meta/s/100/200/300", target.Key)
}
