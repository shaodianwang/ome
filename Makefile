# Define the directory containing the charts
CHARTS_DIR := ./charts

# Define the registry and image tagging
REGISTRY     ?= ghcr.io/moirai-internal
TAG          ?= $(GIT_TAG)
ARCH         ?= linux/amd64
MANAGER_IMG  ?= $(REGISTRY)/ome-manager:$(TAG)

# Git version and commit information for build
version_pkg = github.com/ome-projects/ome/pkg/version
GIT_TAG ?= $(shell git describe --tags --dirty --always)
LD_FLAGS += -X '$(version_pkg).GitVersion=$(GIT_TAG)'
LD_FLAGS += -X '$(version_pkg).GitCommit=$(shell git rev-parse HEAD)'

# Get the currently used Golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
	GOBIN=$(shell go env GOPATH)/bin
else
	GOBIN=$(shell go env GOBIN)
endif

# Go command configurations
GO_CMD ?= go
GO_FMT ?= gofmt
# Use go.mod go version as a single source of truth for the Go version
GO_VERSION := $(shell awk '/^go /{print $$2}' go.mod|head -n1)

# Go build cache configuration
# Only use custom cache when CUSTOM_GO_CACHE is set to avoid conflicts with CI caching
ifdef CUSTOM_GO_CACHE
GOCACHE ?= $(shell pwd)/.cache/go-build
GOMODCACHE ?= $(shell pwd)/.cache/go-mod
export GOCACHE
export GOMODCACHE

# Ensure cache directories exist
$(shell mkdir -p $(GOCACHE) $(GOMODCACHE))
endif

# Determine Docker build command (use nerdctl if available)
DOCKER_BUILD_CMD ?= docker

# Check if nerdctl is available and use it if it exists
ifeq ($(shell command -v nerdctl 2> /dev/null),)
    # nerdctl not found, using docker (already set above)
else
    DOCKER_BUILD_CMD = nerdctl
endif

# Enable Docker BuildKit for cache mounts
export DOCKER_BUILDKIT=1

# CRD Options
CRD_OPTIONS ?= "crd:maxDescLen=0"

# Self-signed CA configuration
OME_ENABLE_SELF_SIGNED_CA ?= false

# ENVTEST K8s version configuration
ENVTEST_K8S_VERSION = 1.30

# Image configuration for success and error scenarios
SUCCESS_200_ISVC_IMG ?= success-200-isvc
ERROR_404_ISVC_IMG ?= error-404-isvc

# Local binary installation path
LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p $(LOCALBIN)

# CPU/Memory limits for controller-manager
OME_CONTROLLER_CPU_LIMIT ?= 100m
OME_CONTROLLER_MEMORY_LIMIT ?= 300Mi
$(shell perl -pi -e 's/cpu:.*/cpu: $(OME_CONTROLLER_CPU_LIMIT)/' config/default/manager_resources_patch.yaml)
$(shell perl -pi -e 's/memory:.*/memory: $(OME_CONTROLLER_MEMORY_LIMIT)/' config/default/manager_resources_patch.yaml)


# By default, wait for the OME controller pod to become ready
WAIT_FOR_CONTROLLER ?= true

.PHONY: all
all: test ## 🎯 Run all tests
	@echo "🎯 Running all tests..."

##@ General

# The help target prints out all targets with their descriptions organized
# beneath their categories. The categories are represented by '##@' and the
# target descriptions by '##'. The awk command is responsible for reading the
# entire set of makefiles included in this invocation, looking for lines of the
# file as xyz: ## something, and then pretty-format the target and help. Then,
# if there's a line with ##@ something, that gets pretty-printed as a category.
# More info on the usage of ANSI control characters for terminal formatting:
# https://en.wikipedia.org/wiki/ANSI_escape_code#SGR_parameters
# More info on the awk command:
# http://linuxcommand.org/lc3_adv_awk.php

##@ 📚 Documentation
.PHONY: help
help: ## 📖 Display this help
	@awk 'BEGIN {FS = ":.*##"; printf "\n📚 Usage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-25s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

.PHONY: generate-apiref
generate-apiref: genref ## 📚 Generate API reference documentation
	@echo "📚 Generating API reference documentation..."
	@cd $(PROJECT_DIR)/hack/genref/ && $(GENREF) -o $(PROJECT_DIR)/site/content/en/docs/reference
	@echo "✅ API reference documentation generated"

include Makefile-deps.mk

##@ 🛠️  Development

.PHONY: manifests
manifests: controller-gen yq ## 📄 Generate WebhookConfiguration, ClusterRole and CustomResourceDefinition objects.
	@echo "\n📦 Kubernetes Manifest Generation Starting..."

	@echo "\n🔧 Step 1: Generating CRD manifests..."
	@$(CONTROLLER_GEN) $(CRD_OPTIONS) paths=./pkg/apis/ome/... output:crd:dir=config/crd/full
	@echo "✅ CRD manifests generated"

	@echo "\n🔑 Step 2: Generating RBAC manifests..."
	@$(CONTROLLER_GEN) rbac:roleName=ome-manager-role paths=./pkg/controller/... output:rbac:artifacts:config=config/rbac
	@echo "✅ RBAC manifests generated"

	@echo "\n📝 Step 3: Generating object boilerplate..."
	@$(CONTROLLER_GEN) object:headerFile="hack/boilerplate.go.txt" paths=./pkg/apis/ome/v1beta1
	@echo "✅ Object boilerplate generated"

	@echo "\n🔄 Step 4: Applying CRD fixes and modifications..."
	@echo "  • Fixing stored versions..."
	@perl -pi -e 's/storedVersions: null/storedVersions: []/g' config/crd/full/ome.io_inferenceservices.yaml
	@echo "  • Fixing conditions..."
	@perl -pi -e 's/conditions: null/conditions: []/g' config/crd/full/ome.io_inferenceservices.yaml
	@echo "  • Updating type definitions..."
	@perl -pi -e 's/Any/string/g' config/crd/full/ome.io_inferenceservices.yaml
	@echo "  • Updating framework properties..."
	@$(YQ) 'del(.spec.versions[0].schema.openAPIV3Schema.properties.spec.properties.*.properties.*.required)' -i config/crd/full/ome.io_inferenceservices.yaml
	@echo "  • Optimizing CRD size..."
	@$(YQ) 'del(.spec.versions[0].schema.openAPIV3Schema.properties.spec.properties.*.properties.ephemeralContainers)' -i config/crd/full/ome.io_inferenceservices.yaml
	@echo "  • Updating probe configurations..."
	@$(YQ) 'del(.spec.versions[0].schema.openAPIV3Schema.properties.spec.properties.*.properties.*.properties.readinessProbe.properties.httpGet.required)' -i config/crd/full/ome.io_inferenceservices.yaml
	@$(YQ) 'del(.spec.versions[0].schema.openAPIV3Schema.properties.spec.properties.*.properties.*.properties.livenessProbe.properties.httpGet.required)' -i config/crd/full/ome.io_inferenceservices.yaml
	@$(YQ) 'del(.spec.versions[0].schema.openAPIV3Schema.properties.spec.properties.*.properties.*.properties.readinessProbe.properties.tcpSocket.required)' -i config/crd/full/ome.io_inferenceservices.yaml
	@$(YQ) 'del(.spec.versions[0].schema.openAPIV3Schema.properties.spec.properties.*.properties.*.properties.livenessProbe.properties.tcpSocket.required)' -i config/crd/full/ome.io_inferenceservices.yaml
	@$(YQ) 'del(.spec.versions[0].schema.openAPIV3Schema.properties.spec.properties.*.properties.containers.items.properties.livenessProbe.properties.httpGet.required)' -i config/crd/full/ome.io_inferenceservices.yaml
	@$(YQ) 'del(.spec.versions[0].schema.openAPIV3Schema.properties.spec.properties.*.properties.containers.items.properties.readinessProbe.properties.httpGet.required)' -i config/crd/full/ome.io_inferenceservices.yaml
	@echo "  • Optimizing InferenceReplica CRD size..."
	@# InferenceReplica embeds PodTemplateSpec one level deeper than ISVC
	@# (spec.runners.items.template.spec.<podfield>). The same OpenAPI
	@# strictness fixes the ISVC CRD needs are applied here at the deeper
	@# path. Without ephemeralContainers deletion the CRD bloats from
	@# ~1.3k to ~4k lines for almost no operator value.
	@$(YQ) 'del(.spec.versions[0].schema.openAPIV3Schema.properties.spec.properties.runners.items.properties.template.properties.spec.properties.ephemeralContainers)' -i config/crd/full/ome.io_inferencereplicas.yaml
	@$(YQ) 'del(.spec.versions[0].schema.openAPIV3Schema.properties.spec.properties.runners.items.properties.template.properties.spec.properties.containers.items.properties.readinessProbe.properties.httpGet.required)' -i config/crd/full/ome.io_inferencereplicas.yaml
	@$(YQ) 'del(.spec.versions[0].schema.openAPIV3Schema.properties.spec.properties.runners.items.properties.template.properties.spec.properties.containers.items.properties.livenessProbe.properties.httpGet.required)' -i config/crd/full/ome.io_inferencereplicas.yaml
	@$(YQ) 'del(.spec.versions[0].schema.openAPIV3Schema.properties.spec.properties.runners.items.properties.template.properties.spec.properties.containers.items.properties.readinessProbe.properties.tcpSocket.required)' -i config/crd/full/ome.io_inferencereplicas.yaml
	@$(YQ) 'del(.spec.versions[0].schema.openAPIV3Schema.properties.spec.properties.runners.items.properties.template.properties.spec.properties.containers.items.properties.livenessProbe.properties.tcpSocket.required)' -i config/crd/full/ome.io_inferencereplicas.yaml
	@$(YQ) 'del(.spec.versions[0].schema.openAPIV3Schema.properties.spec.properties.runners.items.properties.template.properties.spec.properties.initContainers.items.properties.readinessProbe.properties.httpGet.required)' -i config/crd/full/ome.io_inferencereplicas.yaml
	@$(YQ) 'del(.spec.versions[0].schema.openAPIV3Schema.properties.spec.properties.runners.items.properties.template.properties.spec.properties.initContainers.items.properties.livenessProbe.properties.httpGet.required)' -i config/crd/full/ome.io_inferencereplicas.yaml
	@echo "  • Preserving Runner template metadata (annotations + labels)..."
	@# controller-gen renders PodTemplateSpec.metadata as a bare {type: object}
	@# with no properties. CRD pruning then silently drops every key under
	@# .spec.runners[].template.metadata on Update — including the per-Component
	@# annotations + labels the IR projector stamps from the parent ISVC. Without
	@# this fix, no-image-bump rollout triggers (annotation-only ISVC edits) never
	@# reach the pod template and the revision hash never flips.
	@$(YQ) '.spec.versions[0].schema.openAPIV3Schema.properties.spec.properties.runners.items.properties.template.properties.metadata.properties = {"annotations": {"type": "object", "additionalProperties": {"type": "string"}}, "labels": {"type": "object", "additionalProperties": {"type": "string"}}}' -i config/crd/full/ome.io_inferencereplicas.yaml
	@echo "  • Setting protocol defaults..."
	@$(YQ) '.spec.versions[0].schema.openAPIV3Schema.properties.spec.properties | .. | select(has("protocol")) | path' config/crd/full/ome.io_inferenceservices.yaml -o j | jq -r '. | map(select(numbers)="["+tostring+"]") | join(".")' | awk '{print "."$$0".protocol.default"}' | xargs -n1 -I{} $(YQ) '{} = "TCP"' -i config/crd/full/ome.io_inferenceservices.yaml
	@$(YQ) '.spec.versions[0].schema.openAPIV3Schema.properties.spec.properties | .. | select(has("protocol")) | path' config/crd/full/ome.io_clusterservingruntimes.yaml -o j | jq -r '. | map(select(numbers)="["+tostring+"]") | join(".")' | awk '{print "."$$0".protocol.default"}' | xargs -n1 -I{} $(YQ) '{} = "TCP"' -i config/crd/full/ome.io_clusterservingruntimes.yaml
	@$(YQ) '.spec.versions[0].schema.openAPIV3Schema.properties.spec.properties | .. | select(has("protocol")) | path' config/crd/full/ome.io_servingruntimes.yaml -o j | jq -r '. | map(select(numbers)="["+tostring+"]") | join(".")' | awk '{print "."$$0".protocol.default"}' | xargs -n1 -I{} $(YQ) '{} = "TCP"' -i config/crd/full/ome.io_servingruntimes.yaml
	@$(YQ) '.spec.versions[0].schema.openAPIV3Schema.properties.spec.properties | .. | select(has("protocol")) | path' config/crd/full/ome.io_inferencereplicas.yaml -o j | jq -r '. | map(select(numbers)="["+tostring+"]") | join(".")' | awk '{print "."$$0".protocol.default"}' | xargs -n1 -I{} $(YQ) '{} = "TCP"' -i config/crd/full/ome.io_inferencereplicas.yaml
	@echo "✅ CRD modifications complete"

	@echo "\n📋 Step 5: Generating minimal CRDs..."
	@./hack/minimal-crdgen.sh
	@echo "✅ Minimal CRDs generated"

	@echo "\n📁 Step 6: Copying manifests to Helm charts..."
	@cp config/crd/full/ome* charts/ome-crd/templates/ && cp config/rbac/role.yaml charts/ome-resources/templates/ome-controller/rbac/role.yaml
	@echo "✅ Manifests copied to Helm charts"

	@echo "\n🎉 Manifest generation completed successfully!\n"

.PHONY: generate
generate: controller-gen ## 🔄 Generate code containing DeepCopy, DeepCopyInto, and DeepCopyObject method implementations and client-go libraries.
	@echo "\n📦 Code Generation Process Starting..."
	@echo "\n🔧 Step 1: Setting up Go environment..."
	@# Use an inline GOFLAGS export instead of `go env -w`, which writes
	@# to ~/.config/go/env. That path is read-only in many sandboxed
	@# code-agent environments and made this target fail with cryptic
	@# permission errors. Inline export is per-recipe and survives to the
	@# sub-scripts invoked below.
	@echo "✅ Go environment configured"

	@echo "\n🔄 Step 2: Generating Kubernetes client-go code..."
	@if ! GOFLAGS=-mod=mod hack/update-codegen.sh 2>generate.err; then \
		echo "❌ Error during code generation:"; \
		cat generate.err; \
		rm generate.err; \
		exit 1; \
	fi
	@rm -f generate.err
	@echo "✅ Client-go code generation complete"

	@echo "\n📝 Step 3: Generating OpenAPI specifications..."
	@if ! GOFLAGS=-mod=mod hack/update-openapigen.sh 2>openapi.err; then \
		echo "❌ Error during OpenAPI generation:"; \
		cat openapi.err; \
		rm openapi.err; \
		exit 1; \
	fi
	@rm -f openapi.err
	@echo "✅ OpenAPI generation complete"

	@echo "\n🎉 Code generation process completed successfully!\n"

.PHONY: generate-python-sdk
generate-python-sdk: generate ## 🔄 Generate OME python SDK.
	@echo "\n📋 Step 4: Generating Python SDK..."
	@if ! hack/python-sdk/client-gen.sh 2>pythonclientgen.err; then \
  		echo "❌ Error during Python SDK generation:"; \
		cat pythonclientgen.err; \
		rm pythonclientgen.err; \
		exit 1; \
	fi
	@rm -f pythonclientgen.err
	@echo "\n🎉 PythonSDK generation complete"

.PHONY: fmt
fmt: install-goimports ## 🧹 Run go fmt and goimports against code
	@echo "🧹 Formatting Go code..."
	@$(GO_CMD) fmt ./...
	@echo "🧹 Organizing imports in Go files..."
	@find . -name '*.go' -not -path '*/vendor/*' -not -path '*/.cache/*' -not -exec grep -q '// Code generated' {} \; -exec $(GOIMPORTS) -w {} +
	@echo "✅ Formatting complete"

.PHONY: vet
vet: ## 🔍 Run go vet against code
	@echo "🔍 Checking code with go vet..."
	@$(GO_CMD) vet -structtag=false -unsafeptr=false ./...
	@echo "✅ Vet checks passed"

.PHONY: tidy
tidy: ## 📦 Run go mod tidy
	@echo "📦 Tidying Go modules..."
	@$(GO_CMD) mod tidy
	@echo "✅ Dependencies cleaned up"

.PHONY: clean-cache
clean-cache: ## 🧹 Clean Go build cache
	@echo "🧹 Cleaning Go build cache..."
ifdef CUSTOM_GO_CACHE
	@rm -rf .cache/go-build .cache/go-mod
endif
	@$(GO_CMD) clean -cache -modcache
	@echo "✅ Cache cleaned"

.PHONY: ci-lint
ci-lint: golangci-lint ## 🔎 Run golangci-lint against code.
	@echo "🔎 Running golangci-lint..."
	$(GOLANGCI_LINT) run --timeout 15m0s
	@echo "✅ Linting complete"

.PHONY: lint-fix
lint-fix: golangci-lint ## 🔧 Run golangci-lint against code and fix linting issues.
	@echo "🔧 Running golangci-lint with auto-fix..."
	$(GOLANGCI_LINT) run --fix --timeout 15m0s
	@echo "✅ Auto-fix complete"

.PHONY: helm-lint
helm-lint: helm ## ⎈ Lint all charts
	@echo "⎈ Linting Helm charts..."
	@for chart in $(CHARTS_DIR)/*/; do \
	  echo "🔍 Linting $$chart..."; \
	  if ! $(HELM) lint $$chart; then \
	    echo "❌ Error: Linting failed for $$chart" >&2; \
	    exit 1; \
	  fi \
	done
	@echo "✅ Helm lint complete"

.PHONY: helm-doc
helm-doc: helm-docs ## 📚 Generate Helm chart documentation via helm-docs
	@echo "📚 Generating Helm documentation..."
	$(HELM_DOCS) --chart-search-root=charts --output-file=README.md
	@echo "✅ Documentation generated"

.PHONY: helm-version-update
helm-version-update: yq ## 🔄 Update Helm chart version
	@echo "🔄 Updating Helm chart versions..."
	@for chart in $(CHARTS_DIR)/*/; do \
		echo "📝 Updating $$chart..."; \
		$(YQ) e -i '.version = "$(GIT_TAG)"' "$${chart}/Chart.yaml"; \
		$(YQ) e -i '.appVersion = "$(GIT_TAG)"' "$${chart}/Chart.yaml"; \
	done
	@echo "✅ Version updates complete"

##@ 🛠️  Build

.PHONY: ome-manager
ome-manager: xet-build ## 🏗️  Build ome-manager binary.
	@echo "🏗️  Building ome-manager..."
	$(GO_BUILD_ENV) $(GO_CMD) build -ldflags="$(LD_FLAGS)" -o bin/manager ./cmd/manager
	@echo "✅ Build complete"

.PHONY: ome-agent
ome-agent: xet-build ## 🔄 Build ome-agent binary.
	@echo "🔄 Building ome-agent..."
	$(GO_BUILD_ENV) $(GO_CMD) build -ldflags="$(LD_FLAGS)" -o bin/ome-agent ./cmd/ome-agent
	@echo "✅ Build complete"

.PHONY: run-ome-manager
run-ome-manager: manifests generate fmt vet ## Run ome-manager binary from local host against the configured Kubernetes cluster in ~/.kube/config or KUBECONFIG env.
	@echo "🏃‍♂️ Running ome-manager..."
	$(GO_BUILD_ENV) $(GO_CMD) run ./cmd/manager/main.go

.PHONY: run-ome-agent-hf-download
run-ome-agent-hf-download: fmt vet ome-agent ## Run ome-agent binary from local host against the configured Kubernetes cluster in ~/.kube/config or KUBECONFIG env.
	@echo "🏃‍♂️ Running ome-agent hf-download..."
	bin/ome-agent hf-download -d -c config/ome-agent/ome-agent.yaml


.PHONY: run-ome-agent-fine-tuned-adapter
run-ome-agent-fine-tuned-adapter: fmt vet ome-agent ## Run ome-agent binary from local host against the configured Kubernetes cluster in ~/.kube/config or KUBECONFIG env.
	@echo "🏃‍♂️ Running ome-agent fine-tuned-adapter..."
	bin/ome-agent fine-tuned-adapter -d -c config/ome-agent/ome-agent.yaml

.PHONY: run-ome-agent-replica
run-ome-agent-replica: fmt vet ome-agent ## Run ome-agent binary from local host against the configured Kubernetes cluster in ~/.kube/config or KUBECONFIG env.
	@echo "🏃‍♂️ Running ome-agent replica..."
	bin/ome-agent replica -d -c config/ome-agent/ome-agent.yaml

.PHONY: ome-image
ome-image: fmt vet ## Build ome-manager image.
	@echo "🚀 Building ome-manager image..."
	$(DOCKER_BUILD_CMD) build --platform=$(ARCH) \
		--build-arg VERSION=$(GIT_TAG) \
		--build-arg GIT_TAG=$(GIT_TAG) \
		--build-arg GIT_COMMIT=$(shell git rev-parse HEAD) \
		. -f dockerfiles/manager.Dockerfile -t $(MANAGER_IMG)
	@echo "✅ Image built"

.PHONY: ome-agent-image
ome-agent-image: fmt vet xet-build ## Build ome-agent image.
	@echo "🚀 Building ome-agent image..."
	$(DOCKER_BUILD_CMD) build --platform=$(ARCH) \
		--build-arg VERSION=$(GIT_TAG) \
		--build-arg GIT_TAG=$(GIT_TAG) \
		--build-arg GIT_COMMIT=$(shell git rev-parse HEAD) \
		. -f dockerfiles/ome-agent.Dockerfile -t $(REGISTRY)/ome-agent:$(TAG)
	@echo "✅ Image built"

.PHONY: muse-server-image
muse-server-image: ## Build the MUSE read-plane server image.
	@echo "🚀 Building muse-server image..."
	$(DOCKER_BUILD_CMD) build --platform=$(ARCH) \
		. -f dockerfiles/muse-server.Dockerfile -t $(REGISTRY)/muse-server:$(TAG)
	@echo "✅ Image built"

.PHONY: muse-web-image
muse-web-image: ## Build the MUSE console (web) image.
	@echo "🚀 Building muse-web image..."
	$(DOCKER_BUILD_CMD) build --platform=$(ARCH) \
		. -f dockerfiles/muse-web.Dockerfile -t $(REGISTRY)/muse-web:$(TAG)
	@echo "✅ Image built"

.PHONY: build-muse-images
build-muse-images: ## 🚀 Build both MUSE images for the current architecture
	@echo "🚀 Building MUSE images for $(ARCH)..."
	@$(MAKE) muse-server-image
	@$(MAKE) muse-web-image
	@echo "✅ MUSE images built"

.PHONY: docker-buildx-setup
docker-buildx-setup: ## 🔧 Setup Docker buildx for multi-arch builds
	@echo "🔧 Setting up Docker buildx..."
	@docker buildx create --name ome-builder --use --platform=linux/amd64,linux/arm64 || docker buildx use ome-builder
	@docker buildx inspect --bootstrap
	@echo "✅ Docker buildx ready for multi-arch builds"

.PHONY: build-all-images
build-all-images: fmt vet ## 🚀 Build all images for current architecture
	@echo "🚀 Building all OME images for $(ARCH)..."
	@$(MAKE) ome-image
	@$(MAKE) ome-agent-image
	@echo "✅ All images built successfully"

.PHONY: build-all-images-multiarch
build-all-images-multiarch: fmt vet docker-buildx-setup ## 🌍 Build all images for multiple architectures
	@echo "🌍 Building all OME images for linux/amd64,linux/arm64..."
	$(DOCKER_BUILD_CMD) buildx build --platform=linux/amd64,linux/arm64 \
		--build-arg VERSION=$(GIT_TAG) \
		--build-arg GIT_TAG=$(GIT_TAG) \
		--build-arg GIT_COMMIT=$(shell git rev-parse HEAD) \
		. -f dockerfiles/manager.Dockerfile -t $(MANAGER_IMG) --push
	$(DOCKER_BUILD_CMD) buildx build --platform=linux/amd64,linux/arm64 \
		--build-arg VERSION=$(GIT_TAG) \
		--build-arg GIT_TAG=$(GIT_TAG) \
		--build-arg GIT_COMMIT=$(shell git rev-parse HEAD) \
		. -f dockerfiles/ome-agent.Dockerfile -t $(REGISTRY)/ome-agent:$(TAG) --push
	@echo "✅ All multi-arch images built and pushed"

.PHONY: telepresence
telepresence: ## 🌐 Setup telepresence
	@echo "🌐 Configuring Telepresence for local development..."
	@hack/telepresence-setup.sh
	@echo "✅ Telepresence ready - happy coding!"

##@ 🚀 Deployment

ifndef ignore-not-found
  ignore-not-found = false
endif

.PHONY: delete-webhooks
delete-webhooks: ## 🧹 Delete validation/mutation webhook configurations
	@echo "🧹 Deleting ValidatingWebhookConfigurations..."
	@$(YQ) eval 'select(.kind == "ValidatingWebhookConfiguration").metadata.name' config/webhook/manifests.yaml | \
		grep -v '^---$$' | \
		xargs -I{} sh -c 'if kubectl delete validatingwebhookconfigurations.admissionregistration.k8s.io {} 2>/dev/null; then echo "✅ Successfully deleted {}"; else echo "⚠️  Not found - {}"; fi'
	@echo "\n🧹 Deleting MutatingWebhookConfigurations..."
	@$(YQ) eval 'select(.kind == "MutatingWebhookConfiguration").metadata.name' config/webhook/manifests.yaml | \
		grep -v '^---$$' | \
		xargs -I{} sh -c 'if kubectl delete mutatingwebhookconfigurations.admissionregistration.k8s.io {} 2>/dev/null; then echo "✅ Successfully deleted {}"; else echo "⚠️  Not found - {}"; fi'
	@echo "\nWebhook cleanup completed with clear status!"

.PHONY: install
install: kustomize ## 🚀 Deploy controller in the configured Kubernetes cluster in ~/.kube/config or KUBECONFIG env.
	@echo "\n📦 OME Deployment Process Starting..."
	@echo "Current KUBECONFIG: $(KUBECONFIG)"
	@echo "## 💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥 ##"
	@echo "Are you really sure you want to completely re-install in [$(value KUBECONFIG)] environment ?"
	@echo "## 💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥 ##"
	@read -p "Press enter to continue" var

	@echo "\n🔧 Step 1: Configuring certificates..."
	@cd config/default && if [ ${OME_ENABLE_SELF_SIGNED_CA} != false ]; then \
		echo "  • Using self-signed CA"; \
		echo > ../certmanager/certificate.yaml; \
	else \
		echo "  • Using certmanager certificate"; \
		git checkout HEAD -- ../certmanager/certificate.yaml; \
	fi
	@echo "✅ Certificate configuration complete"

	@echo "\n🚀 Step 2: Renewing cert manager..."
	kubectl -n cert-manager rollout restart deployment cert-manager
	kubectl -n cert-manager rollout restart deployment cert-manager-webhook
	kubectl -n cert-manager rollout restart deployment cert-manager-cainjector

	@echo "\n🚀 Step 3: Deploying OME components..."
	@echo "  • Applying kustomize configuration..."
	kubectl apply --server-side --force-conflicts -k config/default

	@if [ ${OME_ENABLE_SELF_SIGNED_CA} != false ]; then \
		echo "  • Setting up self-signed CA..."; \
		./hack/self-signed-ca.sh; \
	fi

	@if [ ${WAIT_FOR_CONTROLLER} = true ]; then \
		echo "\n⏳ Step 4: Waiting for OME controller to be ready..."; \
		kubectl wait --for=condition=ready pod -l control-plane=ome-controller-manager -n ome --timeout=300s; \
	fi

	@echo "\n🧹 Step 5: Cleanup..."
	@git checkout HEAD -- config/certmanager/certificate.yaml
	@echo "✅ Cleanup complete"

	@echo "\n🎉 OME deployment completed successfully!\n"

.PHONY: uninstall
uninstall: kustomize ## 🧹 Uninstall controller from the configured Kubernetes cluster in ~/.kube/config or KUBECONFIG env.
	@echo "Current KUBECONFIG: $(KUBECONFIG)"
	@echo "## 💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥 ##"
	@echo "Are you really sure you want to completely destroy [$(value KUBECONFIG)] environment ?"
	@echo "## 💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥 ##"
	@read -p "Press enter to continue" var
	kubectl delete --ignore-not-found=$(ignore-not-found) -k config/default
	@echo "✅ Controller uninstalled"

.PHONY: kustomize-validate
kustomize-validate: kustomize ## 🔍 Validate kustomize configuration without applying to cluster
	@echo "\n🔍 Validating kustomize configuration..."
	@cd config/default && $(KUSTOMIZE) build --load-restrictor=LoadRestrictionsNone . > /dev/null && echo "✅ Default configuration is valid" || (echo "❌ Error in default configuration" && exit 1)
	@echo "\n✅ Kustomize validation completed successfully!\n"

.PHONY: push-manager-image
push-manager-image: ome-image ## Push manager image to registry.
	@echo "🚀 Pushing manager image to registry..."
	$(DOCKER_BUILD_CMD) push $(MANAGER_IMG)
	@echo "✅ Image pushed"

.PHONY: push-ome-agent-image
push-ome-agent-image: ome-agent-image ## Push ome-agent image to registry.
	@echo "🚀 Pushing ome-agent image to registry..."
	$(DOCKER_BUILD_CMD) push $(REGISTRY)/ome-agent:$(TAG)
	@echo "✅ Image pushed"

.PHONY: patch-manager-dev
patch-manager-dev: push-manager-image ## Deploy manager image to dev cluster.
	@echo "🔄 Patching manager image to dev cluster..."
	echo "Patch manager image to dev: $(MANAGER_IMG)"
	./hack/patch_image_dev.sh $(MANAGER_IMG) manager
	@echo "✅ Patch complete"

.PHONY: deploy-helm
deploy-helm: manifests helm ## Deploy OME using Helm
	@echo "🚀 Deploying OME using Helm..."
	helm install ome-crd charts/ome-crd/ --wait --timeout 180s
	helm install ome charts/ome-resources/ --wait --timeout 180s
	@echo "✅ Deployment complete"

.PHONY: artifacts
artifacts: kustomize ## Generate artifacts for release.
	@echo "📦 Generating artifacts..."
	@mkdir -p artifacts
	$(KUSTOMIZE) build config/default -o artifacts/manifests.yaml
	@echo "✅ Artifacts generated"

##@ 🧪 Testing

# --- Configuration ---
GOTOOLCHAIN      ?= go1.25.0+auto # https://github.com/golang/go/issues/75031
XET_LIB_PATH     := $(shell pwd)/pkg/xet/target/release
EXCLUDE_PATTERNS := "pkg/testing/|pkg/testutils/|_generated\.go|zz_generated|pkg/apis/|pkg/openapi/|pkg/client/|pkg/modelconfig/examples/"

# Define test packages
TEST_PACKAGES     := $(shell go list ./... | grep -v -E '(pkg/apis|pkg/testing|pkg/openapi|pkg/client)')
CMD_PACKAGES      := $(shell go list ./cmd/...)
CMD_PACKAGES_NO_XET := $(shell go list ./cmd/... | grep -v './cmd/ome-agent')
PKG_PACKAGES      := $(shell go list ./pkg/... | grep -v -E '(pkg/testing|pkg/openapi|pkg/client|pkg/xet)')
INTERNAL_PACKAGES := $(shell go list ./internal/...)

# Centralized Environment
TEST_ENV = \
	KUBEBUILDER_ASSETS="$(shell $(ENVTEST) use $(ENVTEST_K8S_VERSION) -p path)" \
	LD_LIBRARY_PATH="$(XET_LIB_PATH):$$LD_LIBRARY_PATH" \
	DYLD_LIBRARY_PATH="$(XET_LIB_PATH):$$DYLD_LIBRARY_PATH" \
	GOTOOLCHAIN=$(GOTOOLCHAIN)

# --- Reusable Test Macro ---
# $(1): Package list variable
# $(2): Output suffix (e.g., cmd, pkg, internal)
# $(3): Optional suffix for filename (e.g., -no-xet)
define run_go_test
	@echo "🧪 Running $(2) tests $(3)..."
	@$(TEST_ENV) $(GO_CMD) test $(1) \
		-coverprofile=coverage-$(2)$(3).out.tmp \
		--covermode=atomic
	@grep -v -E $(EXCLUDE_PATTERNS) coverage-$(2)$(3).out.tmp > coverage-$(2)$(3).out
	@rm coverage-$(2)$(3).out.tmp
	@echo "✅ $(2) tests passed $(3)"
endef

.PHONY: xet-build
xet-build: ## 🔧 Build XET library
	@echo "🔧 Building XET library..."
	@cd pkg/xet && $(MAKE) build
	@echo "✅ XET library built"

.PHONY: test
test: fmt vet manifests envtest xet-build ## 🧪 Run all tests with coverage
	@echo "\n🧪 Running comprehensive test suite (Toolchain: $(GOTOOLCHAIN))..."
	$(call run_go_test,$(CMD_PACKAGES),cmd)
	$(call run_go_test,$(PKG_PACKAGES),pkg)
	$(call run_go_test,$(INTERNAL_PACKAGES),internal)
	@echo "\n🎉 All tests completed successfully!"

.PHONY: test-no-xet
test-no-xet: fmt vet manifests envtest ## 🧪 Run tests excluding ome-agent
	@echo "\n🧪 Running test suite (excluding ome-agent, Toolchain: $(GOTOOLCHAIN))..."
	$(call run_go_test,$(CMD_PACKAGES_NO_XET),cmd,-no-xet)
	$(call run_go_test,$(PKG_PACKAGES),pkg,-no-xet)
	$(call run_go_test,$(INTERNAL_PACKAGES),internal,-no-xet)
	@echo "\n🎉 All tests completed successfully (excluding ome-agent)!"

.PHONY: coverage
coverage: ## Show coverage summary and enforce threshold
	@echo "\n---------- Coverage Summary ----------"
	@for part in cmd pkg internal; do \
		echo "\n$$part Coverage:"; \
		go tool cover -func=coverage-$$part.out | grep -v "100.0%"; \
	done
	@echo "\nTotal Coverage:"
	@cmd_cov=$$(go tool cover -func=coverage-cmd.out | grep total | awk '{sub(/%/,"",$$3); print $$3}'); \
	pkg_cov=$$(go tool cover -func=coverage-pkg.out | grep total | awk '{sub(/%/,"",$$3); print $$3}'); \
	int_cov=$$(go tool cover -func=coverage-internal.out | grep total | awk '{sub(/%/,"",$$3); print $$3}'); \
	echo "CMD: $$cmd_cov%"; echo "PKG: $$pkg_cov%"; echo "Internal: $$int_cov%"; \
	avg_cov=$$(awk "BEGIN {printf \"%.2f\", ($$cmd_cov + $$pkg_cov + $$int_cov) / 3}"); \
	echo "\nAverage Coverage: $$avg_cov%"; \
	if awk "BEGIN {exit !($$avg_cov < 48)}"; then \
		echo "❌ Average coverage $$avg_cov% is below threshold of 48%"; \
		exit 1; \
	fi
.PHONY: dep-crds
dep-crds: keda-crds gateway-api-crds scheduler-plugins-crds kueue-crds ## 🧪 Vendor optional-CRD YAMLs from go.mod modules

DEP_CRDS_DIR := tests/integration/dep-crds

# Each <bundle>-crds target: copies CRD YAMLs from the corresponding
# go.mod module into tests/integration/dep-crds/<bundle>/. Tests point
# at these directories (envtest via framework.OptionalCRDBundle, KIND via
# kindfix CRDPaths). The module-vendored bundles (keda, gateway-api,
# scheduler-plugins) are gitignored and regenerated here — NOT committed;
# only the hand-vendored bundles (envoy-gateway, prometheus-operator) are
# tracked, with a SOURCE file. `make integration-test` runs dep-crds as a
# prerequisite, and CI's dep-crds-drift step runs it to catch a forgotten
# regen after a go.mod bump. Maintainers run it locally when bumping a dep.
#
# Pattern follows kueue/Makefile-deps.mk: locate via `go list -m
# -mod=readonly`, copy via `cp -f`, never write to GOPATH.

.PHONY: keda-crds
keda-crds: ## Vendor github.com/kedacore/keda CRDs (keda.sh — ScaledObject, ScaledJob, TriggerAuthentication, ClusterTriggerAuthentication)
	@mkdir -p $(DEP_CRDS_DIR)/keda
	@KEDA_DIR=$$(go list -m -mod=readonly -f "{{.Dir}}" github.com/kedacore/keda/v2 2>/dev/null) ; \
	if [ -z "$$KEDA_DIR" ]; then echo "github.com/kedacore/keda/v2 not in go.mod; skipping keda-crds" >&2; exit 0; fi ; \
	cp -f "$$KEDA_DIR/config/crd/bases/"*.yaml $(DEP_CRDS_DIR)/keda/ ; \
	echo "✅ Vendored KEDA CRDs to $(DEP_CRDS_DIR)/keda/"

.PHONY: gateway-api-crds
gateway-api-crds: ## Vendor sigs.k8s.io/gateway-api standard-channel CRDs (HTTPRoute)
	@mkdir -p $(DEP_CRDS_DIR)/gateway-api
	@GA_DIR=$$(go list -m -mod=readonly -f "{{.Dir}}" sigs.k8s.io/gateway-api 2>/dev/null) ; \
	if [ -z "$$GA_DIR" ]; then echo "sigs.k8s.io/gateway-api not in go.mod; skipping gateway-api-crds" >&2; exit 0; fi ; \
	cp -f "$$GA_DIR/config/crd/standard/gateway.networking.k8s.io_httproutes.yaml" $(DEP_CRDS_DIR)/gateway-api/ ; \
	echo "✅ Vendored Gateway API CRDs to $(DEP_CRDS_DIR)/gateway-api/"

.PHONY: scheduler-plugins-crds
scheduler-plugins-crds: ## Vendor sigs.k8s.io/scheduler-plugins CRDs (scheduling.x-k8s.io — PodGroup, for KIND gang specs)
	@mkdir -p $(DEP_CRDS_DIR)/scheduler-plugins
	@SP_DIR=$$(go list -m -mod=readonly -f "{{.Dir}}" sigs.k8s.io/scheduler-plugins 2>/dev/null) ; \
	if [ -z "$$SP_DIR" ]; then echo "sigs.k8s.io/scheduler-plugins not in go.mod; skipping scheduler-plugins-crds" >&2; exit 0; fi ; \
	cp -f "$$SP_DIR/config/crd/bases/scheduling.x-k8s.io_podgroups.yaml" $(DEP_CRDS_DIR)/scheduler-plugins/ ; \
	echo "✅ Vendored scheduler-plugins CRDs to $(DEP_CRDS_DIR)/scheduler-plugins/"

.PHONY: kueue-crds
kueue-crds: ## Vendor sigs.k8s.io/kueue CRDs (kueue.x-k8s.io — ResourceFlavor, ClusterQueue, LocalQueue, Workload, … for the OEP-0018 real-Kueue envtest harness)
	@mkdir -p $(DEP_CRDS_DIR)/kueue
	@KUEUE_DIR=$$(go list -m -mod=readonly -f "{{.Dir}}" sigs.k8s.io/kueue 2>/dev/null) ; \
	if [ -z "$$KUEUE_DIR" ]; then echo "sigs.k8s.io/kueue not in go.mod; skipping kueue-crds" >&2; exit 0; fi ; \
	cp -f "$$KUEUE_DIR/config/components/crd/bases/"*.yaml $(DEP_CRDS_DIR)/kueue/ ; \
	echo "✅ Vendored Kueue CRDs to $(DEP_CRDS_DIR)/kueue/"

.PHONY: integration-test
integration-test: fmt vet manifests envtest dep-crds ## 🧪 Run integration tests
	@echo "🧪 Running integration tests..."
	@# Most test packages under tests/ are Ginkgo suites (per
	@# .claude/integration-test-framework-design.md §7). Each Ginkgo
	@# package has a single TestXxx(t) entry that calls util.RunSuite
	@# which registers Gomega's fail handler with ginkgo.RunSpecs.
	@#
	@# Excluded packages:
	@#   - tests/integration/kindutil/* AND tests/perf/*  — plain testing.T
	@#     packages, NOT Ginkgo suites (they do not import ginkgo, so its
	@#     flags are unregistered). -ginkgo.trace below would error with
	@#     `flag provided but not defined: -ginkgo.trace` and fail the
	@#     binary before any test runs. tests/perf/* runs in its own CI step
	@#     via the `test-perf` target (the scale harness + rangespec check).
	@#   - tests/integration/.../*_kind  — KIND-backed suites (real
	@#     kubelet via Docker): omenative_coordination_kind, irmanaged_kind,
	@#     rawdeployment_kind, omenative_perm_kind. Run locally via the
	@#     `make test-kind*` targets; not wired into envtest CI because
	@#     mks-cpu agents don't expose a Docker daemon. The `_kind$$` filter
	@#     below excludes ANY such suite, so a newly added KIND suite is
	@#     dropped automatically instead of hanging the envtest run.
	@#
	@# Verbosity policy: -ginkgo.v is OFF by default. With it on, Ginkgo
	@# flushes the per-spec GinkgoWriter for every passing spec, which
	@# turns the framework.SetupTestLogger output (controller-runtime
	@# logs at ErrorLevel) into a wall of red text on green runs.
	@# Without -ginkgo.v, passing specs collapse to "•" and Ginkgo still
	@# dumps the full GinkgoWriter on failures — so the failure trace
	@# you actually need is preserved via -ginkgo.trace.
	@# Set GINKGO_VERBOSE=1 to opt back in (or TEST_LOG_LEVEL=-1 for debug).
	@#
	@# Timeout: go test defaults to 10m PER PACKAGE. 20m gives headroom
	@# without masking a genuine deadlock — any real loop bug would
	@# still hang past 20m.
	@PKGS=$$(go list ./tests/... | grep -v -E '/kindutil($$|/)|_kind$$|/tests/perf/') ; \
	$(TEST_ENV) go test -v -timeout=20m $$PKGS $(if $(GINKGO_VERBOSE),-ginkgo.v) -ginkgo.trace
	@echo "✅ Integration tests passed"

# Perf scenario selection. Defaults to the baseline (large, for beefy/local
# runs); the CI step overrides these with the smaller ci/ scenario that converges
# on a constrained agent. Override on the CLI to run a custom scenario.
PERF_GENERATOR ?= tests/perf/inferenceservice/configs/baseline/generator.yaml
PERF_RANGESPEC ?= tests/perf/inferenceservice/configs/baseline/rangespec.yaml

.PHONY: run-perf-isvc
run-perf-isvc: envtest dep-crds ## 🏎️  Run the local InferenceService scale harness (envtest + fake executor, no kubelet).
	@echo "🏎️  Running InferenceService perf harness ($(PERF_GENERATOR))..."
	@mkdir -p $(PROJECT_DIR)/artifacts/perf-isvc
	@$(TEST_ENV) go run ./tests/perf/inferenceservice/runner \
		-o $(PROJECT_DIR)/artifacts/perf-isvc \
		-generator-config $(PERF_GENERATOR) \
		-project-root $(PROJECT_DIR)

.PHONY: test-perf-isvc
test-perf-isvc: run-perf-isvc ## 🏁 Run the perf harness, then check results against the rangespec.
	@echo "🏁 Checking perf results against rangespec ($(PERF_RANGESPEC))..."
	@# -range must be absolute: `go test` runs the checker binary from its own
	@# package dir, not the repo root, so a repo-relative path would not resolve.
	@go test ./tests/perf/inferenceservice/checker/... -args \
		-summary $(PROJECT_DIR)/artifacts/perf-isvc/summary.yaml \
		-cmd-stats $(PROJECT_DIR)/artifacts/perf-isvc/cmd.stats.yaml \
		-range $(PROJECT_DIR)/$(PERF_RANGESPEC)

# OMENative perf scenario selection. Mirrors PERF_GENERATOR/PERF_RANGESPEC but
# for the OMENative / InferenceReplica harness (which drives REAL pods to Ready
# via the fake kubelet, so its cost scales with pod count). Override on the CLI.
PERF_OMEN_GENERATOR ?= tests/perf/omenative/configs/baseline/generator.yaml
PERF_OMEN_RANGESPEC ?= tests/perf/omenative/configs/baseline/rangespec.yaml

.PHONY: run-perf-omenative
run-perf-omenative: envtest dep-crds ## 🏎️  Run the local OMENative/IR pod-reconcile scale harness (envtest + fake kubelet).
	@echo "🏎️  Running OMENative perf harness ($(PERF_OMEN_GENERATOR))..."
	@mkdir -p $(PROJECT_DIR)/artifacts/perf-omenative
	@$(TEST_ENV) go run ./tests/perf/omenative/runner \
		-o $(PROJECT_DIR)/artifacts/perf-omenative \
		-generator-config $(PERF_OMEN_GENERATOR) \
		-project-root $(PROJECT_DIR)

.PHONY: test-perf-omenative
test-perf-omenative: run-perf-omenative ## 🏁 Run the OMENative perf harness, then check results against the rangespec.
	@echo "🏁 Checking OMENative perf results against rangespec ($(PERF_OMEN_RANGESPEC))..."
	@# -range must be absolute: go test runs the checker binary from its own pkg dir.
	@go test ./tests/perf/omenative/checker/... -args \
		-summary $(PROJECT_DIR)/artifacts/perf-omenative/summary.yaml \
		-cmd-stats $(PROJECT_DIR)/artifacts/perf-omenative/cmd.stats.yaml \
		-range $(PROJECT_DIR)/$(PERF_OMEN_RANGESPEC)

# Multi-cluster PLACEMENT perf scenario selection. Baseline stands up 1 control
# plane + N member apiservers in one process; the CI step overrides to the
# smaller ci/ scenario. Override on the CLI.
PERF_MC_GENERATOR ?= tests/perf/multicluster/configs/baseline/generator.yaml
PERF_MC_RANGESPEC ?= tests/perf/multicluster/configs/baseline/rangespec.yaml

.PHONY: run-perf-multicluster
run-perf-multicluster: envtest dep-crds ## 🏎️  Run the local multi-cluster placement harness (1 control plane + N member envtests).
	@echo "🏎️  Running multi-cluster placement perf harness ($(PERF_MC_GENERATOR))..."
	@mkdir -p $(PROJECT_DIR)/artifacts/perf-multicluster
	@$(TEST_ENV) go run ./tests/perf/multicluster/runner \
		-o $(PROJECT_DIR)/artifacts/perf-multicluster \
		-generator-config $(PERF_MC_GENERATOR)

.PHONY: test-perf-multicluster
test-perf-multicluster: run-perf-multicluster ## 🏁 Run the placement harness, then check results against the rangespec.
	@echo "🏁 Checking placement perf results against rangespec ($(PERF_MC_RANGESPEC))..."
	@# -range must be absolute: go test runs the checker binary from its own pkg dir.
	@go test ./tests/perf/multicluster/checker/... -args \
		-summary $(PROJECT_DIR)/artifacts/perf-multicluster/summary.yaml \
		-cmd-stats $(PROJECT_DIR)/artifacts/perf-multicluster/cmd.stats.yaml \
		-range $(PROJECT_DIR)/$(PERF_MC_RANGESPEC)

.PHONY: test-perf
test-perf: ## 🏎️  Perf CI step (separate from integration-test): harness unit tests + the CI-sized scale harnesses + rangespec checks.
	@echo "🧪 Running perf-harness unit tests (plain testing.T, no ginkgo flags)..."
	@$(TEST_ENV) go test -timeout=10m ./tests/perf/...
	@# CI agents are constrained, so run the small ci/ scenarios (the large
	@# baselines do not converge in the recorder window on an mks-cpu pod).
	@$(MAKE) test-perf-isvc \
		PERF_GENERATOR=tests/perf/inferenceservice/configs/ci/generator.yaml \
		PERF_RANGESPEC=tests/perf/inferenceservice/configs/ci/rangespec.yaml
	@$(MAKE) test-perf-omenative \
		PERF_OMEN_GENERATOR=tests/perf/omenative/configs/ci/generator.yaml \
		PERF_OMEN_RANGESPEC=tests/perf/omenative/configs/ci/rangespec.yaml
	@$(MAKE) test-perf-multicluster \
		PERF_MC_GENERATOR=tests/perf/multicluster/configs/ci/generator.yaml \
		PERF_MC_RANGESPEC=tests/perf/multicluster/configs/ci/rangespec.yaml

.PHONY: mc-realkueue
mc-realkueue: envtest dep-crds ## 🧪 Run the OEP-0018 real-Kueue multi-cluster validation suite (opt-in, envtest)
	@echo "🧪 Running real-Kueue multi-cluster validation (OME_MC_REALKUEUE=1)..."
	@# Heavy: 1 control-plane + 2 member envtest apiservers in one process, each
	@# member running a full OME manager + Kueue's admission stack (core controllers
	@# + scheduler + pod webhook). Gated behind OME_MC_REALKUEUE so the default
	@# `make integration-test` run leaves members bare (the sim path in
	@# placement_test.go / gc_test.go). No kubelet: admission = Kueue gate removal.
	@# CI runs this soft-fail, like test-kind. Narrow with GINKGO_FOCUS='S1|S3'.
	@OME_MC_REALKUEUE=1 $(TEST_ENV) \
		go test ./tests/integration/multicluster/... -run TestMultiCluster -timeout 20m \
		$(if $(GINKGO_FOCUS),-ginkgo.focus '$(GINKGO_FOCUS)') $(if $(GINKGO_VERBOSE),-v -ginkgo.v)

.PHONY: test-kind
test-kind: xet-build dep-crds ## 🧪 Run KIND-backed integration tests (real kubelet — needs docker + kind locally)
	@echo "🧪 Running KIND-backed integration tests..."
	@command -v kind >/dev/null 2>&1 || { \
		echo "❌ kind is not installed. Install with: go install sigs.k8s.io/kind@v0.27.0"; \
		exit 1; \
	}
	@docker info >/dev/null 2>&1 || { \
		echo "❌ docker is not reachable. KIND tests need a running Docker daemon."; \
		exit 1; \
	}
	@# KIND_KEEP_CLUSTER=true keeps the cluster between runs for fast
	@# iteration. Set KIND_RECREATE=true to force a fresh cluster.
	@# GINKGO_FOCUS='regex' narrows to specific specs (e.g. 'PD per-Component').
	@# 90m timeout: 94 specs in the suite with per-spec timeouts up to
	@# kindLongTimeout=5m. Headroom for the long tail without masking a
	@# genuine deadlock.
	@LD_LIBRARY_PATH="$(XET_LIB_PATH):$$LD_LIBRARY_PATH" \
		DYLD_LIBRARY_PATH="$(XET_LIB_PATH):$$DYLD_LIBRARY_PATH" \
		KIND_KEEP_CLUSTER=$${KIND_KEEP_CLUSTER:-true} \
		go test ./tests/integration/singlecluster/controller/omenative_coordination_kind/... \
		-v -timeout 90m $(if $(GINKGO_FOCUS),-ginkgo.focus '$(GINKGO_FOCUS)') -ginkgo.v
	@echo "✅ KIND integration tests passed"

.PHONY: test-kind-irmanaged
test-kind-irmanaged: xet-build dep-crds ## 🧪 Run KIND-backed IR-managed OMENative tests (real kubelet — needs docker + kind locally)
	@echo "🧪 Running KIND IR-managed OMENative tests..."
	@command -v kind >/dev/null 2>&1 || { \
		echo "❌ kind is not installed. Install with: go install sigs.k8s.io/kind@v0.27.0"; \
		exit 1; \
	}
	@docker info >/dev/null 2>&1 || { \
		echo "❌ docker is not reachable. KIND tests need a running Docker daemon."; \
		exit 1; \
	}
	@# Exercises the IR-managed path (the production default since #320),
	@# which test-kind's omenative_coordination_kind suite deliberately disables. Uses
	@# its OWN cluster (ome-irmanaged-kind) with the inferencereplica
	@# controller registered + IR-managed enabled. On a resource-constrained
	@# host (e.g. an 8GB Colima VM) run `make test-kind-clean` first so this
	@# cluster doesn't contend with the coordination cluster.
	@LD_LIBRARY_PATH="$(XET_LIB_PATH):$$LD_LIBRARY_PATH" \
		DYLD_LIBRARY_PATH="$(XET_LIB_PATH):$$DYLD_LIBRARY_PATH" \
		KIND_KEEP_CLUSTER=$${KIND_KEEP_CLUSTER:-true} \
		go test ./tests/integration/singlecluster/controller/irmanaged_kind/... \
		-v -timeout 30m $(if $(GINKGO_FOCUS),-ginkgo.focus '$(GINKGO_FOCUS)') -ginkgo.v
	@echo "✅ KIND IR-managed tests passed"

.PHONY: test-kind-raw
test-kind-raw: xet-build dep-crds ## 🧪 Run KIND-backed RawDeployment tests (real kubelet — needs docker + kind locally)
	@echo "🧪 Running KIND RawDeployment tests..."
	@command -v kind >/dev/null 2>&1 || { \
		echo "❌ kind is not installed. Install with: go install sigs.k8s.io/kind@v0.27.0"; \
		exit 1; \
	}
	@docker info >/dev/null 2>&1 || { \
		echo "❌ docker is not reachable. KIND tests need a running Docker daemon."; \
		exit 1; \
	}
	@# RawDeployment end-to-end on a real kubelet (its own cluster ome-raw-kind):
	@# the Deployment must actually schedule pods that report Ready, which the
	@# envtest controller/inferenceservice suite cannot observe.
	@LD_LIBRARY_PATH="$(XET_LIB_PATH):$$LD_LIBRARY_PATH" \
		DYLD_LIBRARY_PATH="$(XET_LIB_PATH):$$DYLD_LIBRARY_PATH" \
		KIND_KEEP_CLUSTER=$${KIND_KEEP_CLUSTER:-true} \
		go test ./tests/integration/singlecluster/controller/rawdeployment_kind/... \
		-v -timeout 20m $(if $(GINKGO_FOCUS),-ginkgo.focus '$(GINKGO_FOCUS)') -ginkgo.v
	@echo "✅ KIND RawDeployment tests passed"

PERM_KUBECONFIG ?= /tmp/kind-ome-coord.kubeconfig
PERM_PROCS ?= 2

.PHONY: test-kind-perm-up
test-kind-perm-up: dep-crds ## 🧪 Bring up the perm KIND cluster + CRDs + fake-serving images (no specs); then run the external manager
	@command -v kind >/dev/null 2>&1 || { \
		echo "❌ kind is not installed. Install with: go install sigs.k8s.io/kind@v0.27.0"; \
		exit 1; \
	}
	@docker info >/dev/null 2>&1 || { \
		echo "❌ docker is not reachable. KIND tests need a running Docker daemon."; \
		exit 1; \
	}
	@# PERM_SETUP_ONLY makes the suite entrypoint bring up the cluster + CRDs +
	@# fake-serving images via RunKindSuite, then return BEFORE RunSpecs.
	@# -count=1 defeats the go-test result cache (otherwise a second `-up` is a
	@# no-op and never re-runs RunKindSuite).
	@echo "🧪 Bringing up perm KIND cluster (ome-coord-kind) — cluster + CRDs + images, no specs..."
	@KIND_KEEP_CLUSTER=true PERM_SETUP_ONLY=1 \
		go test -count=1 -v -timeout 20m -run TestOMENativePerm \
		./tests/integration/singlecluster/controller/omenative_perm_kind/
	@kind get kubeconfig --name ome-coord-kind > "$(PERM_KUBECONFIG)"
	@echo ""
	@echo "✅ Cluster up. Kubeconfig written to $(PERM_KUBECONFIG)"
	@echo ""
	@echo "Next — in a SEPARATE terminal, run the OME manager against it"
	@echo "(default IR-managed path; coordination is gated there):"
	@echo "    KUBECONFIG=$(PERM_KUBECONFIG) make run-ome-manager"
	@echo ""
	@echo "Then drive the parallel rollout specs:"
	@echo "    make test-kind-perm"

.PHONY: test-kind-perm
test-kind-perm: dep-crds ## 🧪 Run the watch-driven OMENative rollout specs in parallel (needs test-kind-perm-up + an external manager)
	@command -v kind >/dev/null 2>&1 || { \
		echo "❌ kind is not installed. Install with: go install sigs.k8s.io/kind@v0.27.0"; \
		exit 1; \
	}
	@kind get clusters 2>/dev/null | grep -qx ome-coord-kind || { \
		echo "❌ KIND cluster 'ome-coord-kind' not found. Run 'make test-kind-perm-up' first."; \
		exit 1; \
	}
	@# Watch-driven, EXTERNAL-manager suite (RunKindSuite Setup=nil): the OME
	@# controller must already be running against the cluster (see
	@# test-kind-perm-up output). Specs are pure drivers, so we run them in
	@# PARALLEL via the ginkgo CLI (go run, since there is no vendored binary).
	@# PERM_PROCS defaults to 2 (== Workers); raise it only with a bigger
	@# cluster — each PD spec brings up a ~10-pod fleet. KIND_KEEP_CLUSTER keeps
	@# the shared cluster alive across the parallel processes.
	@echo "🧪 Driving OMENative rollout specs ($(PERM_PROCS) parallel procs)..."
	@KIND_KEEP_CLUSTER=true \
		go run github.com/onsi/ginkgo/v2/ginkgo \
		-procs=$(PERM_PROCS) --timeout=40m -v \
		$(if $(GINKGO_FOCUS),--focus '$(GINKGO_FOCUS)') \
		./tests/integration/singlecluster/controller/omenative_perm_kind/
	@echo "✅ OMENative rollout specs passed"

.PHONY: test-kind-clean
test-kind-clean: ## 🧹 Delete leftover KIND clusters from prior test-kind runs
	@kind get clusters 2>/dev/null | while read -r c; do \
		echo "🧹 Deleting kind cluster: $$c"; \
		kind delete cluster --name "$$c"; \
	done


.PHONY: site-server
site-server: hugo ## 🌐 Start Hugo development server
	@echo "🌐 Starting Hugo development server..."
	@cd site && $(HUGO) server
	@echo "✅ Hugo server started"
