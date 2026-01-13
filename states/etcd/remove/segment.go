package remove

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

type SegmentParam struct {
	framework.ParamBase `use:"remove segment" desc:"Remove segment from meta with specified filters"`
	SegmentID           int64  `name:"segmentID" default:"0" desc:"segment id to remove"`
	CollectionID        int64  `name:"collectionID" default:"0" desc:"collection id to filter with"`
	State               string `name:"state" default:"" desc:"segment state"`
	MaxNum              int64  `name:"maxNum" default:"9223372036854775807" desc:"max number of segment to remove"`
	Run                 bool   `name:"run" default:"false" desc:"flag to control actually run or dry"`
}

func (c *ComponentRemove) RemoveSegmentCommand(ctx context.Context, p *SegmentParam) error {
	backupDir := fmt.Sprintf("segments-backup_%d", time.Now().UnixMilli())

	filterFunc := func(segmentInfo *datapb.SegmentInfo) bool {
		return (p.CollectionID == 0 || segmentInfo.CollectionID == p.CollectionID) &&
			(p.SegmentID == 0 || segmentInfo.GetID() == p.SegmentID) &&
			(p.State == "" || strings.EqualFold(segmentInfo.State.String(), p.State))
	}

	removedCnt := 0
	dryRunCount := 0
	opFunc := func(info *datapb.SegmentInfo) error {
		// dry run, display segment first
		if !p.Run {
			dryRunCount++
			fmt.Printf("dry run segment:%d collectionID:%d state:%s\n", info.ID, info.CollectionID, info.State.String())
			return nil
		}

		if err := c.backupSegmentInfo(info, backupDir); err != nil {
			return err
		}

		if err := common.RemoveSegment(ctx, c.client, c.basePath, info); err != nil {
			fmt.Printf("Remove segment %d from Etcd failed, err: %s\n", info.ID, err.Error())
			return err
		}

		removedCnt++
		fmt.Printf("Remove segment %d from etcd succeeds.\n", info.GetID())
		return nil
	}

	err := common.WalkAllSegments(ctx, c.client, c.basePath, filterFunc, opFunc, p.MaxNum)
	if err != nil && !errors.Is(err, common.ErrReachMaxNumOfWalkSegment) {
		fmt.Printf("WalkAllSegmentsfailed, err: %s\n", err.Error())
	}

	if !p.Run {
		fmt.Println("dry run segments, total count:", dryRunCount)
	} else {
		fmt.Println("Remove segments succeeds, total count:", removedCnt)
	}
	return nil
}

func (c *ComponentRemove) backupSegmentInfo(info *datapb.SegmentInfo, backupDir string) error {
	if _, err := os.Stat(backupDir); errors.Is(err, os.ErrNotExist) {
		err := os.MkdirAll(backupDir, os.ModePerm)
		if err != nil {
			fmt.Println("Failed to create folder,", err.Error())
			return err
		}
	}

	now := time.Now()
	filePath := fmt.Sprintf("%s/bw_etcd_segment_%d.%s.bak", backupDir, info.GetID(), now.Format("060102-150405"))
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		fmt.Println("failed to open backup segment file", err.Error())
		return err
	}

	defer f.Close()

	bs, err := proto.Marshal(info)
	if err != nil {
		fmt.Println("failed to marshal backup segment", err.Error())
		return err
	}

	_, err = f.Write(bs)
	return err
}
