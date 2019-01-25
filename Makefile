export REPOSITORY=flinkk8soperator
include boilerplate/lyft/docker_build/Makefile
include boilerplate/lyft/golang_test_targets/Makefile

.PHONY: update_boilerplate
update_boilerplate:
	@boilerplate/update.sh

.PHONY: compile
compile:
	go build -o flinkk8soperator ./cmd/flinkk8soperator/ && mv ./flinkk8soperator ${GOPATH}/bin

.PHONY: linux_compile
linux_compile:
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o /artifacts/flinkk8soperator ./cmd/flinkk8soperator/

all: compile
