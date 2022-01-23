help: ## Show help
	@grep -E '(^[0-9a-zA-Z_-]+:.*?##.*$$)|(^##)' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[32m%-25s\033[0m %s\n", $$1, $$2}' | sed -e 's/\[32m##/[33m/'


generate-mocks: ## Generate mocks
	go generate ./...

run-linters: ## Run linters
	golangci-lint run

fmt: ## Format code
	goimports -w -local github.com/ezotrank/ ./.

.PHONY: test
test: ## Run tests
	go test -v -count=1 ./...

install-dev-deps: ## Install development dependencies
ifeq ($(shell which mockgen),)
	@echo "mockgen not found, installing dependencies"
	go install github.com/golang/mock/mockgen@v1.6.0
endif
ifeq (, $(shell which golangci-lint))
	@echo "golangci-lint not found, installing dependencies"
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.43.0
endif
ifeq (, $(shell which goimports))
	echo "goimports not found, installing dependencies"
	go install golang.org/x/tools/cmd/goimports@latest
endif