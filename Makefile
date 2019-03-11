export REPOSITORY=flinkk8soperator
include boilerplate/lyft/docker_build/Makefile
include boilerplate/lyft/golang_test_targets/Makefile

.PHONY: generate
generate:
	tmp/codegen/update-generated.sh

.PHONY: update_boilerplate
update_boilerplate:
	@boilerplate/update.sh

.PHONY: compile
compile: generate
	go build -o flinkk8soperator ./cmd/flinkk8soperator/ && mv ./flinkk8soperator ${GOPATH}/bin

.PHONY: linux_compile
linux_compile: generate
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o /artifacts/flinkk8soperator ./cmd/flinkk8soperator/

gen-config:
	which pflags || (go get github.com/lyft/flytestdlib/cli/pflags)
	@go generate ./...

all: compile
