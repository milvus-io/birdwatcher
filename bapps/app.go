package bapps

import (
	"log"

	"github.com/milvus-io/birdwatcher/framework"
)

// BApp interface for birdwatcher application
type BApp interface {
	Run(framework.State)
}

// AppOption application setup option function.
type AppOption func(*appOption)

type appOption struct {
	logger *log.Logger
}

// WithLogger returns AppOption to setup application logger.
func WithLogger(logger *log.Logger) AppOption {
	return func(opt *appOption) {
		opt.logger = logger
	}
}
