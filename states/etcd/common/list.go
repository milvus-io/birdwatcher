package common

import (
	"context"
	"fmt"

	"github.com/golang/protobuf/proto"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/runtime/protoiface"
)

// ListProtoObjects returns proto objects with specified prefix.
func ListProtoObjects[T any, P interface {
	*T
	protoiface.MessageV1
}](ctx context.Context, cli *clientv3.Client, prefix string, filters ...func(t *T) bool) ([]T, []string, error) {
	resp, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, nil, err
	}
	result := make([]T, 0, len(resp.Kvs))
	keys := make([]string, 0, len(resp.Kvs))
LOOP:
	for _, kv := range resp.Kvs {
		var elem T
		info := P(&elem)
		err = proto.Unmarshal(kv.Value, info)
		if err != nil {
			fmt.Println(err.Error())
			continue
		}

		for _, filter := range filters {
			if !filter(&elem) {
				continue LOOP
			}
		}
		result = append(result, elem)
		keys = append(keys, string(kv.Key))
	}
	return result, keys, nil
}
