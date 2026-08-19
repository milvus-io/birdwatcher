package models

import (
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

type FieldIndex = ProtoWrapper[*indexpb.FieldIndex]

type SegmentIndex = ProtoWrapper[*indexpb.SegmentIndex]

type StatsTask = ProtoWrapper[*indexpb.StatsTask]
