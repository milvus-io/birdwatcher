GO ?= go

INSTALL_PATH := $(PWD)/bin

# golangci-lint
GOLANGCI_LINT_VERSION := 1.55.2
GOLANGCI_LINT_OUTPUT := $(shell $(INSTALL_PATH)/golangci-lint --version 2>/dev/null)
INSTALL_GOLANGCI_LINT := $(findstring $(GOLANGCI_LINT_VERSION), $(GOLANGCI_LINT_OUTPUT))

getdeps:
	@mkdir -p $(INSTALL_PATH)
	@if [ -z "$(INSTALL_GOLANGCI_LINT)" ]; then \
		echo "Installing golangci-lint into ./bin/" && curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(INSTALL_PATH) v${GOLANGCI_LINT_VERSION} ; \
	else \
		echo "golangci-lint v@$(GOLANGCI_LINT_VERSION) already installed"; \
	fi

all: static-check birdwatcher 

birdwatcher:
	@echo "Compiling birdwatcher"
	@mkdir -p bin
	@CGO_ENABLED=0 go build -o bin/birdwatcher main.go

birdwatcher_wkafka:
	@echo "Compiling birdwatcher with kafka(CGO_ENABLED)"
	@mkdir -p bin
	@CGO_ENABLED=1 go build -o bin/birdwatcher_wkafka -tags WKAFKA main.go

static-check: getdeps
	@echo "Running static-check"
	@$(INSTALL_PATH)/golangci-lint cache clean
	@$(INSTALL_PATH)/golangci-lint run --timeout=10m --config ./.golangci.yml ./...
