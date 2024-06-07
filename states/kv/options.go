package kv

import (
	clientv3 "go.etcd.io/etcd/client/v3"
)

type loadOption struct {
	withKeysOnly bool
}

func (opt *loadOption) EtcdOptions() []clientv3.OpOption {
	var result []clientv3.OpOption
	if opt.withKeysOnly {
		result = append(result, clientv3.WithKeysOnly())
	}
	return result
}

func defaultLoadOption() *loadOption {
	return &loadOption{}
}

type LoadOption func(opt *loadOption)

func WithKeysOnly() LoadOption {
	return func(opt *loadOption) {
		opt.withKeysOnly = true
	}
}
