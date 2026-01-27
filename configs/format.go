package configs

import "os"

const (
	// EnvOutputFormat is the environment variable for global output format
	EnvOutputFormat = "BW_OUTPUT_FORMAT"
)

// GetGlobalOutputFormat resolves the global output format name from:
// 1. Environment variable BW_OUTPUT_FORMAT (highest priority)
// 2. Config file OutputFormat setting
// 3. Default to empty string (caller should use default format)
func (c *Config) GetGlobalOutputFormat() string {
	// Check environment variable first
	if envFormat := os.Getenv(EnvOutputFormat); envFormat != "" {
		return envFormat
	}

	// Check config file
	if c != nil && c.OutputFormat != "" {
		return c.OutputFormat
	}

	// Return empty, caller will use default
	return ""
}
