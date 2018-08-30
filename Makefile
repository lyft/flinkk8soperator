# This is the only target that should be overriden by the project. Get your binary into ${GOREPO}/bin
.PHONY: compile
compile:
	@dep ensure
	@mkdir -p ./bin
	@go build -o flinkk8soperator ./cmd/flinkk8soperator/main.go && mv ./flinkk8soperator ./bin

cross_compile:
	@mkdir -p ./bin
	@GOOS=linux GOARCH=amd64 go build -o flinkk8soperator ./cmd/flinkk8soperator/main.go && mv ./flinkk8soperator ./bin

generate:
	@operator-sdk generate k8s

# lint runs golint, gofmt, and govet
# .PHONY: lint
lint:
	@script/lint
# tests runs all test except those found in ./vendor
.PHONY: tests
tests: lint
	@echo "************************ gotest **********************************"
	@KUBERNETES_CONFIG="$(shell pwd)/testdata/fake_config.yaml" go test -cover ./... -race
	@echo "******************************************************************"
server-mock:
	@KUBERNETES_CONFIG="$(HOME)/.kube/config" go run ./cmd/operator_test/main.go log-source-line
# server starts the service in development mode
.PHONY: server
server:
	@KUBERNETES_CONFIG="$(HOME)/.kube/config" go run ./cmd/flinkk8soperator/main.go log-source-line
.PHONY: clean
clean:
	-@kubectl delete -f deploy/cr.yaml
	-@kubectl delete -f artifacts/operator.yaml
	-@kubectl delete -f deploy/rbac.yaml
	-@kubectl delete -f deploy/crd.yaml
	-@kubectl delete -f deploy/flyte-namespace.yaml
