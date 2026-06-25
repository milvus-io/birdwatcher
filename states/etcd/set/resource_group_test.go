package set

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/rgpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
)

func TestApplyResourceGroupNodeNumsUpdatesOnlyRequestAndLimit(t *testing.T) {
	rg := &querypb.ResourceGroup{
		Name:  "rg-a",
		Nodes: []int64{1, 2},
		Config: &rgpb.ResourceGroupConfig{
			Requests:     &rgpb.ResourceGroupLimit{NodeNum: 1},
			Limits:       &rgpb.ResourceGroupLimit{NodeNum: 3},
			TransferFrom: []*rgpb.ResourceGroupTransfer{{ResourceGroup: "default"}},
			NodeFilter: &rgpb.ResourceGroupNodeFilter{
				NodeLabels: []*commonpb.KeyValuePair{{Key: "rg", Value: "rg-a"}},
			},
		},
	}

	beforeTransfer := rg.GetConfig().GetTransferFrom()[0]
	beforeFilter := rg.GetConfig().GetNodeFilter()

	changed := applyResourceGroupNodeNums(rg, 2, 4)

	require.True(t, changed)
	require.EqualValues(t, 2, rg.GetConfig().GetRequests().GetNodeNum())
	require.EqualValues(t, 4, rg.GetConfig().GetLimits().GetNodeNum())
	require.Same(t, beforeTransfer, rg.GetConfig().GetTransferFrom()[0])
	require.Same(t, beforeFilter, rg.GetConfig().GetNodeFilter())
	require.Equal(t, []int64{1, 2}, rg.GetNodes())
}

func TestApplyResourceGroupNodeNumsCreatesMissingConfig(t *testing.T) {
	rg := &querypb.ResourceGroup{Name: "rg-a"}

	changed := applyResourceGroupNodeNums(rg, 2, 4)

	require.True(t, changed)
	require.EqualValues(t, 2, rg.GetConfig().GetRequests().GetNodeNum())
	require.EqualValues(t, 4, rg.GetConfig().GetLimits().GetNodeNum())
}

func TestApplyResourceGroupNodeNumsSkipsNegativeFields(t *testing.T) {
	rg := &querypb.ResourceGroup{
		Name: "rg-a",
		Config: &rgpb.ResourceGroupConfig{
			Requests: &rgpb.ResourceGroupLimit{NodeNum: 1},
			Limits:   &rgpb.ResourceGroupLimit{NodeNum: 3},
		},
	}

	changed := applyResourceGroupNodeNums(rg, -1, 5)

	require.True(t, changed)
	require.EqualValues(t, 1, rg.GetConfig().GetRequests().GetNodeNum())
	require.EqualValues(t, 5, rg.GetConfig().GetLimits().GetNodeNum())
}
