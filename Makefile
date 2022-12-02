GO ?= go

all: static-check birdwatcher 

birdwatcher:
	@echo "Compiling birdwatch"
	@mkdir -p bin
	@go build -o bin/birdwatcher main.go

static-check:
	@echo "Running static-check"
	@golangci-lint cache clean
	@golangci-lint run --timeout=10m --config ./.golangci.yml ./...
