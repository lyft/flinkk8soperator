# Uses Docker multistage build https://docs.docker.com/develop/develop-images/multistage-build/#use-multi-stage-builds
FROM lyft/go:f3fb33e6adfcf743f50188bd3393e3aa35e42b13 as builder
COPY . ${GOPATH}/src/github.com/lyft/flinkk8soperator
WORKDIR ${GOPATH}/src/github.com/lyft/flinkk8soperator
RUN dep ensure && \
    rm -f flinkk8soperator.bin && \
    go build -o flinkk8soperator.bin ./cmd/flinkk8soperator/main.go && \
    mv ./flinkk8soperator.bin /bin/flinkk8soperator

ENTRYPOINT ["/bin/flinkk8soperator"]
