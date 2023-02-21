GO ?= go

all: static-check birdwatcher 

birdwatcher:
	@echo "Compiling birdwatcher"
	@mkdir -p bin
	@CGO_ENABLED=0 go build -o bin/birdwatcher main.go

birdwatcher_wkafka:
	@echo "Compiling birdwatcher with kafka(CGO_ENABLED)"
	@mkdir -p bin
	@CGO_ENABLED=1 go build -o bin/birdwatcher_wkafka -tags WKAFKA main.go

static-check:
	@echo "Running static-check"
	@golangci-lint cache clean
	@golangci-lint run --timeout=10m --config ./.golangci.yml ./...
