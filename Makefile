GOCMD=go
GOTEST=$(GOCMD) test
GOVET=$(GOCMD) vet
BINARY_NAME=prometheus-sketch


.PHONY: all build vendor

all: help

vendor: 
	$(GOCMD) mod vendor

build:
	GO111MODULE=on $(GOCMD) build -mod vendor -o ./$(BINARY_NAME) ./cmd/

help: 
	@echo ''
	@echo 'Usage:'
	@echo '  ${YELLOW}make${RESET} ${GREEN}<target>${RESET}'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "} { \
		if (/^[a-zA-Z_-]+:.*?##.*$$/) {printf "    ${YELLOW}%-20s${GREEN}%s${RESET}\n", $$1, $$2} \
		else if (/^## .*$$/) {printf "  ${CYAN}%s${RESET}\n", substr($$1,4)} \
		}' $(MAKEFILE_LIST)

clean: ## Remove build related file
	rm -rf $(BINARY_NAME)