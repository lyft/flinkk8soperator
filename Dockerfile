# Uses Docker multistage build https://docs.docker.com/develop/develop-images/multistage-build/#use-multi-stage-builds
FROM lyft/go:0661c9bfde0cc9c248351d51cbdf4a04e0e3863a as builder
COPY . ${GOPATH}/src/github.com/lyft/flinkk8soperator
WORKDIR ${GOPATH}/src/github.com/lyft/flinkk8soperator
RUN dep ensure && \
    rm -f flinkk8soperator.bin && \
    go build -o flinkk8soperator.bin ./cmd/flinkk8soperator/main.go && \
    mv ./flinkk8soperator.bin /bin/flinkk8soperator

WORKDIR /etc/flinkk8soperator/config
ENTRYPOINT ["/bin/flinkk8soperator"]
