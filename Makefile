STORAGE_PLUGIN ?= es-storage-layer

CLUSTERPEDIA_BUILDER_IMAGE = "ghcr.io/clusterpedia-io/clusterpedia/builder"
SUPPORT_CLUSTERPEDIA_VERSIONS = ""

BUILDER_IMAGE ?= ""

VERSION = $(shell git describe --tags 2>/dev/null)
ifeq ($(VERSION),)
	VERSION = v0.0.0
endif

BUILDER_TAG = $(shell echo $(BUILDER_IMAGE)|awk -F ':' '{ print $$2 }')
ifeq ($(BUILDER_TAG),)
	BUILDER_TAG = latest
endif

GOARCH ?= $(shell go env GOARCH)

PWD = $(shell pwd)
CLUSTERPEDIA_REPO ?= $(PWD)/clusterpedia

build-plugin:
	CLUSTERPEDIA_REPO=$(CLUSTERPEDIA_REPO) \
		clusterpedia/hack/builder.sh plugins $(STORAGE_PLUGIN).so

build-components:
	OUTPUT_DIR=$(PWD) ON_PLUGINS=true \
		$(MAKE) -C clusterpedia all

image-plugin:
ifeq ($(BUILDER_IMAGE), "")
	$(error BUILDER_IMAGE is not define)
endif

	docker buildx build \
		-t $(STORAGE_PLUGIN)-$(GOARCH):$(VERSION)-$(BUILDER_TAG) \
		--platform=linux/$(GOARCH) \
		--load \
		--build-arg BUILDER_IMAGE=$(BUILDER_IMAGE) \
		--build-arg PLUGIN_NAME=$(STORAGE_PLUGIN).so .

build-image-with-clusterpedia:
	set -e; \
	for version in $(SUPPORT_CLUSTERPEDIA_VERSION); do \
		BUILDER_IMAGE=$(CLUSTERPEDIA_BUILDER_IMAGE):$$version $(MAKE) image-plugin; \
	done;
