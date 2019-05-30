.PHONY: docker_build
docker_build:
	IMAGE_NAME=$$REPOSITORY ./boilerplate/lyft/docker_build/docker_build.sh

.PHONY: dockerhub_push
dockerhub_push:
	IMAGE_NAME=lyft/$$REPOSITORY REGISTRY=docker.io ./boilerplate/lyft/docker_build/docker_build.sh
