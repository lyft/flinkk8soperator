export REPOSITORY=flinkk8soperator
include boilerplate/lyft/docker_build/Makefile
include boilerplate/lyft/golang_test_targets/Makefile

.PHONY: generate
generate:
	tmp/codegen/update-generated.sh

.PHONY: compile
compile: generate
	mkdir -p ./bin
	go build -o bin/flinkoperator ./cmd/flinkk8soperator/main.go

.PHONY: linux_compile
linux_compile: generate
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o /artifacts/flinkoperator ./cmd/flinkk8soperator/main.go

.PHONY: test_unit_codecov
test_unit_codecov:
	go test ./... -race -coverprofile=coverage.txt -covermode=atomic; curl -s https://codecov.io/bash > codecov_bash.sh; bash codecov_bash.sh

gen-config:
	which pflags || (go get github.com/lyft/flytestdlib/cli/pflags)
	@go generate ./...

all: compile
