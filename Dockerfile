FROM golang:1.20.2-alpine3.17 as builder
RUN apk add git openssh-client make curl bash

# COPY only the dep files for efficient caching
COPY go.mod go.sum /go/src/github.com/lyft/flinkk8soperator/
WORKDIR /go/src/github.com/lyft/flinkk8soperator

# Pull dependencies
RUN go mod download

# COPY the rest of the source code
COPY . /go/src/github.com/lyft/flinkk8soperator/

# This 'linux_compile' target should compile binaries to the /artifacts directory
# The main entrypoint should be compiled to /artifacts/flinkk8soperator
RUN go mod vendor && make linux_compile

# update the PATH to include the /artifacts directory
ENV PATH="/artifacts:${PATH}"

# This will eventually move to centurylink/ca-certs:latest for minimum possible image size
FROM alpine:3.17
COPY --from=builder /artifacts /bin
CMD ["flinkoperator"]
