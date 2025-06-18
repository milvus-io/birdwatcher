package models

import (
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
)

type FieldIndex = ProtoWrapper[*indexpb.FieldIndex]

type SegmentIndex = ProtoWrapper[*indexpb.SegmentIndex]

type StatsTask = ProtoWrapper[*indexpb.StatsTask]
