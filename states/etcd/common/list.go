package common

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/golang/protobuf/proto"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/runtime/protoiface"
)

func ListJSONObjects[T any, P interface{ *T }](ctx context.Context, cli clientv3.KV, prefix string, filters ...func(t P) bool) ([]P, []string, error) {
	resp, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, nil, err
	}
	result := make([]P, 0, len(resp.Kvs))
	keys := make([]string, 0, len(resp.Kvs))
LOOP:
	for _, kv := range resp.Kvs {
		var elem T
		err = json.Unmarshal(kv.Value, &elem)
		if err != nil {
			fmt.Println(err.Error())
			continue
		}

		for _, filter := range filters {
			if !filter(&elem) {
				continue LOOP
			}
		}
		result = append(result, &elem)
		keys = append(keys, string(kv.Key))
	}
	return result, keys, nil
}

// ListProtoObjects returns proto objects with specified prefix.
func ListProtoObjects[T any, P interface {
	*T
	protoiface.MessageV1
}](ctx context.Context, cli clientv3.KV, prefix string, filters ...func(t *T) bool) ([]T, []string, error) {
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

// ListProtoObjectsAdv returns proto objects with specified prefix.
// add preFilter to handle tombstone cases.
func ListProtoObjectsAdv[T any, P interface {
	*T
	protoiface.MessageV1
}](ctx context.Context, cli clientv3.KV, prefix string, preFilter func(string, []byte) bool, filters ...func(t *T) bool) ([]T, []string, error) {
	resp, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, nil, err
	}
	result := make([]T, 0, len(resp.Kvs))
	keys := make([]string, 0, len(resp.Kvs))
LOOP:
	for _, kv := range resp.Kvs {
		if !preFilter(string(kv.Key), kv.Value) {
			continue
		}
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
