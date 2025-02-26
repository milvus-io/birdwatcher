package models

import "github.com/milvus-io/milvus/pkg/v2/proto/datapb"

type ImportJob = ProtoWrapper[*datapb.ImportJob]
type PreImportTask = ProtoWrapper[*datapb.PreImportTask]
type ImportTaskV2 = ProtoWrapper[*datapb.ImportTaskV2]

func NewImportJob(info *datapb.ImportJob, key string) *ImportJob {
	return NewProtoWrapper(info, key)
}
