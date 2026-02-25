package states

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/storage"
)

type StorageAnalysisParam struct {
	framework.ParamBase `use:"storage-analysis" desc:"segment storage analysis" require:"etcd,minio"`
	CollectionID        int64 `name:"collection" default:"0" desc:"collection id to analysis"`
	Detail              bool  `name:"detail" default:"false" desc:"print detailed binlog size info"`
}

func (app *ApplicationState) StorageAnalysisCommand(ctx context.Context, p *StorageAnalysisParam) error {
	state, ok := app.states[etcdTag]
	if !ok {
		return errors.New("Etcd instance not connected")
	}
	etcd, ok := state.(*InstanceState)
	if !ok {
		return errors.New("Etcd instance not connected")
	}
	state, ok = app.states[ossTag]
	if !ok {
		return errors.New("Minio instance not connected")
	}
	minio, ok := state.(*storage.OSSState)
	if !ok {
		return errors.New("Minio instance not connected")
	}

	segments, err := common.ListSegments(ctx, etcd.client, etcd.basePath, func(s *models.Segment) bool {
		return p.CollectionID == 0 || s.CollectionID == p.CollectionID
	})
	if err != nil {
		return err
	}

	for _, segment := range segments {
		fmt.Printf("segment %d\n", segment.ID)
		for _, fieldBinlog := range segment.GetBinlogs() {
			fmt.Println("fieldID:", fieldBinlog.FieldID)
			var size int64
			for _, binlog := range fieldBinlog.Binlogs {
				info, err := minio.Stat(ctx, binlog.LogPath)
				if err != nil {
					fmt.Println("failed to stats", err.Error())
					continue
				}
				if p.Detail {
					fmt.Printf("Binlog %s size %s\n", binlog.LogPath, hrSize(info.Size))
				}
				size += info.Size
			}
			fmt.Printf("Total binlog size: %s\n", hrSize(size))
		}
	}

	return nil
}
