GO ?= go

INSTALL_PATH := $(PWD)/bin
GOLANGCI_LINT_VERSION := 1.55.2
GOLANGCI_LINT_OUTPUT := $(shell $(INSTALL_PATH)/golangci-lint --version 2>/dev/null)
INSTALL_GOLANGCI_LINT := $(findstring $(GOLANGCI_LINT_VERSION), $(GOLANGCI_LINT_OUTPUT))

# gci
GCI_VERSION := 0.11.2
GCI_OUTPUT := $(shell $(INSTALL_PATH)/gci --version 2>/dev/null)
INSTALL_GCI := $(findstring $(GCI_VERSION),$(GCI_OUTPUT))


GOFUMPT_VERSION := 0.5.0
GOFUMPT_OUTPUT := $(shell $(INSTALL_PATH)/gofumpt --version 2>/dev/null)
INSTALL_GOFUMPT := $(findstring $(GOFUMPT_VERSION),$(GOFUMPT_OUTPUT))

all: static-check birdwatcher

birdwatcher:
	@echo "Compiling birdwatcher"
	@mkdir -p bin
	@CGO_ENABLED=0 go build -o bin/birdwatcher main.go

birdwatcher_wkafka:
	@echo "Compiling birdwatcher with kafka(CGO_ENABLED)"
	@mkdir -p bin
	@CGO_ENABLED=1 go build -o bin/birdwatcher_wkafka -tags WKAFKA main.go

getdeps:
	@mkdir -p $(INSTALL_PATH)
	@if [ -z "$(INSTALL_GOLANGCI_LINT)" ]; then \
		echo "Installing golangci-lint into ./bin/" && curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(INSTALL_PATH) v${GOLANGCI_LINT_VERSION} ; \
	else \
		echo "golangci-lint v@$(GOLANGCI_LINT_VERSION) already installed"; \
	fi

static-check: getdeps
	@echo "Running static-check"
	@$(INSTALL_PATH)/golangci-lint cache clean
	@$(INSTALL_PATH)/golangci-lint run --timeout=10m --config ./.golangci.yml ./...

lint-fix: getdeps
	@mkdir -p $(INSTALL_PATH)
	@if [ -z "$(INSTALL_GCI)" ]; then \
		echo "Installing gci v$(GCI_VERSION) to ./bin/" && GOBIN=$(INSTALL_PATH) go install github.com/daixiang0/gci@v$(GCI_VERSION); \
	else \
		echo "gci v$(GCI_VERSION) already installed"; \
	fi
	@if [ -z "$(INSTALL_GOFUMPT)" ]; then \
		echo "Installing gofumpt v$(GOFUMPT_VERSION) to ./bin/" && GOBIN=$(INSTALL_PATH) go install mvdan.cc/gofumpt@v$(GOFUMPT_VERSION); \
	else \
		echo "gofumpt v$(GOFUMPT_VERSION) already installed"; \
	fi
	@echo "Running gofumpt fix"
	@$(INSTALL_PATH)/gofumpt -l -w ./
	@echo "Running gci fix"
	@$(INSTALL_PATH)/gci write ./ --skip-generated -s standard -s default -s "prefix(github.com/milvus-io)" --custom-order
	@echo "Running golangci-lint auto-fix"
	@$(INSTALL_PATH)/golangci-lint cache clean
	@$(INSTALL_PATH)/golangci-lint run --fix --timeout=30m --config ./.golangci.yml;
