package configs

import "os"

var _ ConfigSource = (*envConfigSource)(nil)

type envConfigSource struct {
}

func (e *envConfigSource) Name() string {
	return "env"
}

func (e *envConfigSource) Get(key string) (string, error) {
	value, ok := os.LookupEnv(key)
	if !ok {
		return "", ErrConfigNotFound
	}
	return value, nil
}

func (e *envConfigSource) Set(key, value string) error {
	return os.Setenv(key, value)
}
