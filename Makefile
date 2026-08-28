# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

GO := go
pkgs  = $(shell $(GO) list ./... | grep -v vendor)
GOFMT_RESULT = $(shell gofmt -l cmd/ pkg/ | tr '\n' '\1')

GOARCH ?= $(shell $(GO) env GOARCH)
GOOS ?= $(shell $(GO) env GOOS)

BUILD_PATH := $(shell pwd)/build
BUILD_BIN_PATH := $(BUILD_PATH)/bin/$(GOOS)/$(GOARCH)

GOLANGCI_LINT := $(BUILD_BIN_PATH)/golangci-lint
ADDLICENSE := $(BUILD_BIN_PATH)/addlicense
ADDLICENSE_VERSION ?= v1.1.1

all: presubmit

test:
	@echo ">> running tests"
	@$(GO) test -short -race $(pkgs)

gofmt:
	@echo ">> Running gofmt..."
	@if [ ! -z "$(GOFMT_RESULT)" ]; then \
		echo -e "gofmt has found suggestions for the following files, please run 'gofmt -w [path]' locally to update the formatting:" ;\
		echo $(GOFMT_RESULT) | tr '\1' '\n';\
		exit 1;\
	fi

lint: $(GOLANGCI_LINT)
	$(GOLANGCI_LINT) run --timeout=10m

addlicense: $(ADDLICENSE)
	$(ADDLICENSE) -c "Google LLC" -l apache -y 2025 -v -ignore 'vendor/**' -ignore 'build/**' .

check-license: $(ADDLICENSE)
	$(ADDLICENSE) -check -ignore 'vendor/**' -ignore 'build/**' .

vet:
	@echo ">> vetting code"
	@$(GO) vet $(pkgs)

presubmit: vet test gofmt lint check-license

TAG:=$(shell if [ "$(shell git rev-parse --abbrev-ref HEAD)" = "master" ]; then echo "0.0.0"; else cat VERSION; fi)
REGISTRY?=gcr.io/google-containers
IMAGE=tpu-device-plugin
ARCHITECTURES=amd64 arm64

build:
	cd cmd/tpu; go build tpu.go

container:
	docker build --pull --no-cache -t ${REGISTRY}/${IMAGE}:${TAG} .

push: container
	docker push ${REGISTRY}/${IMAGE}:${TAG}

container-multi-arch:
	@docker buildx inspect img-builder > /dev/null 2>&1 \
		|| docker buildx create --name img-builder --use

	@for arch in $(ARCHITECTURES); do \
		echo ">> Building for architecture: $${arch}"; \
			docker buildx build \
			--platform linux/$${arch} \
			--build-arg ARCH=$${arch} \
			--output=type=docker \
			-t ${REGISTRY}/$(IMAGE)-$${arch}:$(TAG) \
			--pull \
			. ; \
	done


push-all-individual:
	for arch in $(ARCHITECTURES); do \
		docker push ${REGISTRY}/${IMAGE}-$$arch:${TAG}; \
	done

push-multi-arch:
	docker manifest create --amend ${REGISTRY}/${IMAGE}:${TAG} $(shell echo $(ARCHITECTURES) | sed -e "s~[^ ]*~$(REGISTRY)/$(IMAGE)\-&:$(TAG)~g")
	@for arch in $(ARCHITECTURES); do \
		docker manifest annotate --arch $${arch} \
			${REGISTRY}/${IMAGE}:${TAG} $(REGISTRY)/$(IMAGE)-$${arch}:${TAG}; \
	done
	docker manifest push --purge ${REGISTRY}/$(IMAGE):$(TAG)

install.tools: $(GOLANGCI_LINT) $(ADDLICENSE)

$(GOLANGCI_LINT):
	export \
		VERSION=v2.11.4 \
		URL=https://raw.githubusercontent.com/golangci/golangci-lint \
		BINDIR=${BUILD_BIN_PATH} && \
	curl -sfL $$URL/$$VERSION/install.sh | sh -s $$VERSION

$(ADDLICENSE):
	mkdir -p $(BUILD_BIN_PATH)
	GOBIN=$(BUILD_BIN_PATH) $(GO) install github.com/google/addlicense@$(ADDLICENSE_VERSION)

.PHONY: all test gofmt lint vet presubmit build container push install.tools addlicense check-license
