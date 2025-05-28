package models

import "github.com/milvus-io/milvus-proto/go-api/v2/schemapb"

type Function = ProtoWrapper[*schemapb.FunctionSchema]

func NewFunction(functionSchema *schemapb.FunctionSchema, key string) *Function {
	return NewProtoWrapper(functionSchema, key)
}
