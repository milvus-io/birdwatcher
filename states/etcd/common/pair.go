package common

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
