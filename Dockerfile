# Uses Docker multistage build https://docs.docker.com/develop/develop-images/multistage-build/#use-multi-stage-builds
FROM lyft/go:44541065de99c1282f10ce372b04fac55c43e6cd as builder
COPY . ${GOPATH}/src/github.com/lyft/flinkk8soperator
WORKDIR ${GOPATH}/src/github.com/lyft/flinkk8soperator
RUN dep ensure && \
    rm -f flinkk8soperator.bin && \
    go build -o flinkk8soperator.bin ./cmd/flinkk8soperator/main.go && \
    mv ./flinkk8soperator.bin /bin/flinkk8soperator

WORKDIR /etc/flinkk8soperator/config
ENTRYPOINT ["/bin/flinkk8soperator"]
