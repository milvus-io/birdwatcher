package models

import "github.com/milvus-io/milvus-proto/go-api/v2/msgpb"

type Channel struct {
	PhysicalName  string
	VirtualName   string
	StartPosition *msgpb.MsgPosition
}

type MsgPosition = ProtoWrapper[*msgpb.MsgPosition]
