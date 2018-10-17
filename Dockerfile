# Uses Docker multistage build https://docs.docker.com/develop/develop-images/multistage-build/#use-multi-stage-builds
FROM lyft/go:9a5d29303e76158ff30047b9a3d67cd10894eb5a as builder
COPY . ${GOPATH}/src/github.com/lyft/flinkk8soperator
WORKDIR ${GOPATH}/src/github.com/lyft/flinkk8soperator
RUN dep ensure && \
    rm -f flinkk8soperator.bin && \
    go build -o flinkk8soperator.bin ./cmd/flinkk8soperator/main.go && \
    mv ./flinkk8soperator.bin /bin/flinkk8soperator

ENTRYPOINT ["/bin/flinkk8soperator"]
