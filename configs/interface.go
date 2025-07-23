package configs

// ConfigSource defines the interface for configuration sources.
type ConfigSource interface {
	Name() string
	Get(key string) (string, error)
	Set(key, value string) error
}
