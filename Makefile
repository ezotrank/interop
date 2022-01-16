generate-mocks:
	go generate ./...

run-linters:
	golangci-lint run

fmt:
	goimports -w -local github.com/ezotrank/ ./.

.PHONY: test
test:
	go test -v -count=1 ./...

install-dev-deps:
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