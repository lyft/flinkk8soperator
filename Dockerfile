FROM golang:1.12.6-alpine3.9 as builder
RUN apk add git openssh-client make curl bash

COPY boilerplate/lyft/golang_test_targets/dep_install.sh /go/src/github.com/lyft/flinkk8soperator/

# COPY only the dep files for efficient caching
COPY Gopkg.* /go/src/github.com/lyft/flinkk8soperator/
WORKDIR /go/src/github.com/lyft/flinkk8soperator

# Pull dependencies
RUN : \
  && sh dep_install.sh \
  && dep ensure -vendor-only

# COPY the rest of the source code
COPY . /go/src/github.com/lyft/flinkk8soperator/

# This 'linux_compile' target should compile binaries to the /artifacts directory
# The main entrypoint should be compiled to /artifacts/flinkk8soperator
RUN make linux_compile

# update the PATH to include the /artifacts directory
ENV PATH="/artifacts:${PATH}"

# This will eventually move to centurylink/ca-certs:latest for minimum possible image size
FROM alpine:3.9
COPY --from=builder /artifacts /bin
CMD ["flinkoperator"]
