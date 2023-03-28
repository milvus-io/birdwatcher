package common

import "github.com/samber/lo"

// GetKVPair iterates KV pairs to find specified key.
func GetKVPair[T interface {
	GetKey() string
	GetValue() string
}](pairs []T, key string) string {
	for _, pair := range pairs {
		if pair.GetKey() == key {
			return pair.GetValue()
		}
	}
	return ""
}

func KVListMap[T interface {
	GetKey() string
	GetValue() string
}](pairs []T) map[string]string {
	return lo.SliceToMap(pairs, func(pair T) (string, string) {
		return pair.GetKey(), pair.GetValue()
	})
}
