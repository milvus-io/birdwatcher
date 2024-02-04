package show

import (
	"context"
	"fmt"
	"path"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/proto/v2.0/commonpb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/datapb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/etcdpb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/indexpb"
	indexpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/indexpb"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
)

type SegmentIndexParam struct {
	framework.ParamBase `use:"show segment-index" desc:"display segment index information" alias:"segments-index,segment-indexes,segments-indexes"`
	CollectionID        int64 `name:"collection" default:"0"`
	SegmentID           int64 `name:"segment" default:"0"`
}

// SegmentIndexCommand returns show segment-index command.
func (c *ComponentShow) SegmentIndexCommand(ctx context.Context, p *SegmentIndexParam) error {
	segments, err := common.ListSegments(c.client, c.basePath, func(info *datapb.SegmentInfo) bool {
		return (p.CollectionID == 0 || info.CollectionID == p.CollectionID) &&
			(p.SegmentID == 0 || info.ID == p.SegmentID)
	})
	if err != nil {
		return err
	}

	segmentIndexes, err := common.ListSegmentIndex(c.client, c.basePath)
	if err != nil {
		return err
	}
	segmentIndexesV2, err := c.listSegmentIndexV2(ctx)
	if err != nil {
		return err
	}

	indexBuildInfo, err := common.ListIndex(ctx, c.client, c.basePath)
	if err != nil {
		return err
	}

	indexes, _, err := common.ListProtoObjects[indexpbv2.FieldIndex](ctx, c.client, path.Join(c.basePath, "field-index"))
	if err != nil {
		return err
	}
	idIdx := make(map[int64]indexpbv2.FieldIndex)
	for _, idx := range indexes {
		idIdx[idx.IndexInfo.IndexID] = idx
	}

	seg2Idx := make(map[int64][]etcdpb.SegmentIndexInfo)
	seg2Idxv2 := make(map[int64][]indexpbv2.SegmentIndex)
	for _, segIdx := range segmentIndexes {
		idxs, ok := seg2Idx[segIdx.SegmentID]
		if !ok {
			idxs = []etcdpb.SegmentIndexInfo{}
		}

		idxs = append(idxs, segIdx)

		seg2Idx[segIdx.GetSegmentID()] = idxs
	}
	for _, segIdx := range segmentIndexesV2 {
		idxs, ok := seg2Idxv2[segIdx.SegmentID]
		if !ok {
			idxs = []indexpbv2.SegmentIndex{}
		}

		idxs = append(idxs, segIdx)

		seg2Idxv2[segIdx.GetSegmentID()] = idxs
	}

	buildID2Info := make(map[int64]indexpb.IndexMeta)
	for _, info := range indexBuildInfo {
		buildID2Info[info.IndexBuildID] = info
	}
	count := make(map[string]int)

	for _, segment := range segments {
		if segment.State != commonpb.SegmentState_Flushed && segment.GetState() != commonpb.SegmentState_Flushing {
			continue
		}
		fmt.Printf("SegmentID: %d\t State: %s", segment.GetID(), segment.GetState().String())
		segIdxs, ok := seg2Idx[segment.GetID()]
		if !ok {
			// try v2 index information
			segIdxv2, ok := seg2Idxv2[segment.GetID()]
			if !ok {
				fmt.Println("\tno segment index info")
				continue
			}
			for _, segIdx := range segIdxv2 {
				fmt.Printf("\n\tIndexV2 build ID: %d, states %s", segIdx.GetBuildID(), segIdx.GetState().String())
				count[segIdx.GetState().String()]++
				idx, ok := idIdx[segIdx.GetIndexID()]
				if ok {
					fmt.Printf("\t Index Type:%v on Field ID: %d", common.GetKVPair(idx.GetIndexInfo().GetIndexParams(), "index_type"), idx.GetIndexInfo().GetFieldID())
				}
				fmt.Printf("\tSerialized Size: %d\n", segIdx.GetSerializeSize())
				fmt.Printf("\tCurrent Index Version: %d\n", segIdx.GetCurrentIndexVersion())
			}
			fmt.Println()
		}

		for _, segIdx := range segIdxs {
			info, ok := buildID2Info[segIdx.BuildID]
			if !ok {
				fmt.Printf("\tno build info found for id: %d\n", segIdx.BuildID)
				fmt.Println(segIdx.String())
			}
			fmt.Printf("\n\tIndex build ID: %d, state: %s", info.IndexBuildID, info.State.String())
			fmt.Printf("\t Index Type:%v on Field ID: %d", common.GetKVPair(info.GetReq().GetIndexParams(), "index_type"), segIdx.GetFieldID())
			fmt.Printf("\t info.SerializeSize: %d\n", info.GetSerializeSize())
		}
		fmt.Println()
	}
	// Print count statistics
	for idxSta, cnt := range count {
		fmt.Printf("[%s]: %d\t", idxSta, cnt)
	}
	fmt.Println()
	return nil
}

func (c *ComponentShow) listSegmentIndexV2(ctx context.Context) ([]indexpbv2.SegmentIndex, error) {
	prefix := path.Join(c.basePath, "segment-index") + "/"
	result, _, err := common.ListProtoObjects[indexpbv2.SegmentIndex](ctx, c.client, prefix)
	return result, err
}
