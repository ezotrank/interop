generate-mocks:
ifeq ($(shell which mockgen),)
	@echo "mockgen not found, install dependencies"
	go github.com/golang/mock/mockgen@latest
endif
	go generate ./...

run-linters:
ifeq (, $(shell which golangci-lint))
	go mod tidy
	go install github.com/golangci/golangci-lint/cmd/golangci-lint
endif
	golangci-lint run

fmt:
ifeq (, $(shell which goimports))
	go mod tidy
	go install golang.org/x/tools/cmd/goimports
endif
	goimports -w -local github.com/ezotrank/ ./.

.PHONY: test
test:
	go test -v -count=1 ./...