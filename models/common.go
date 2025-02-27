package models

import (
	"strconv"

	"google.golang.org/protobuf/proto"
)

// EnumName returns proto name base on value-name mapping.
func EnumName(m map[int32]string, v int32) string {
	s, ok := m[v]
	if ok {
		return s
	}
	return strconv.Itoa(int(v))
}

type ProtoWrapper[T proto.Message] struct {
	proto T
	key   string
}

func (w *ProtoWrapper[T]) GetProto() T {
	return w.proto
}

func (w *ProtoWrapper[T]) Key() string {
	return w.key
}

func NewProtoWrapper[T proto.Message](p T, key string) *ProtoWrapper[T] {
	return &ProtoWrapper[T]{
		proto: p,
		key:   key,
	}
}
