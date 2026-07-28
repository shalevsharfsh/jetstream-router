CLUSTER  ?= jetstream
IMAGE    ?= jetstream-router:dev
NS       ?= jetstream-router
KEDA_VER ?= v2.17.2

.DEFAULT_GOAL := help
.PHONY: help cluster keda build load deploy demo logs metrics scale-watch chaos test lint down clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

cluster: ## Create the local kind cluster (idempotent)
	@kind get clusters | grep -qx $(CLUSTER) || kind create cluster --name $(CLUSTER)

keda: ## Install KEDA into the cluster
	@kubectl get ns keda >/dev/null 2>&1 || \
		kubectl apply --server-side -f https://github.com/kedacore/keda/releases/download/$(KEDA_VER)/keda-$(KEDA_VER).yaml
	@kubectl -n keda rollout status deploy/keda-operator --timeout=180s

build: ## Build the container image
	docker build -t $(IMAGE) .

load: build ## Load the image into kind (no registry needed)
	kind load docker-image $(IMAGE) --name $(CLUSTER)

deploy: cluster keda load ## Full path: cluster + KEDA + image + manifests
	kubectl apply -f k8s/00-namespace.yaml
	kubectl apply -f k8s/10-infra.yaml
	kubectl apply -f k8s/20-config.yaml
	kubectl -n $(NS) rollout status deploy/nats  --timeout=120s
	kubectl -n $(NS) rollout status deploy/redis --timeout=120s
	kubectl apply -f k8s/30-tap.yaml
	kubectl apply -f k8s/40-workers.yaml
	kubectl -n $(NS) rollout status deploy/tap --timeout=180s
	@echo "waiting for workers to register their durable consumers..."
	@sleep 15
	kubectl apply -f k8s/50-keda.yaml
	@echo
	@echo "deployed. try:  make demo"

demo: ## Tail the alerts every path is producing
	@echo "--- alerts across all four paths (ctrl-c to stop) ---"
	kubectl -n $(NS) logs -l component=worker --all-containers --tail=5 -f --prefix | grep --line-buffered ALERT

logs: ## Tail everything
	kubectl -n $(NS) logs -l component=worker --all-containers --tail=20 -f --prefix

metrics: ## Print the tap's key metrics
	@kubectl -n $(NS) port-forward svc/tap 9090:9090 >/dev/null 2>&1 & \
	sleep 2; \
	curl -s localhost:9090/metrics | grep -E '^jsr_(events|tap)' | grep -v '^#'; \
	kill %1 2>/dev/null || true

scale-watch: ## Watch KEDA scale the paths independently
	kubectl -n $(NS) get pods,scaledobject,hpa -w

chaos: ## Kill the engagement worker; the other paths must not care
	kubectl -n $(NS) delete pod -l app=engagement-worker
	kubectl -n $(NS) get pods

test: ## Run the test suite
	.venv/bin/pytest -q

lint: ## Lint
	.venv/bin/ruff check router tests

down: ## Delete the namespace, keep the cluster
	kubectl delete namespace $(NS) --ignore-not-found

clean: ## Delete the whole kind cluster
	kind delete cluster --name $(CLUSTER)
