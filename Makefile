CLUSTER ?= jetstream
IMAGE   ?= jetstream-router:dev
NS      ?= jetstream-router

.DEFAULT_GOAL := help
.PHONY: help build test race lint run image cluster deploy logs alerts metrics congest down clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

build: ## Compile the binary
	go build -o bin/router ./cmd/router

test: ## Run the tests
	go test ./...

race: ## Run the tests with the race detector
	go test -race ./...

lint: ## Vet and format check
	go vet ./...
	@test -z "$$(gofmt -l . )" || (echo "gofmt needed:"; gofmt -l .; exit 1)

run: build ## Run locally against the live firehose
	CONFIG_PATH=local.config.json ./bin/router

image: ## Build the container image
	docker build -t $(IMAGE) .

cluster: ## Create the kind cluster (idempotent)
	@kind get clusters | grep -qx $(CLUSTER) || kind create cluster --name $(CLUSTER)

deploy: cluster image ## Build, load and apply everything
	kind load docker-image $(IMAGE) --name $(CLUSTER)
	kubectl apply -f deploy/k8s.yaml
	kubectl -n $(NS) rollout status statefulset/router --timeout=180s
	@echo
	@echo "deployed. try:  make alerts"

logs: ## Tail everything
	kubectl -n $(NS) logs -f statefulset/router

alerts: ## Tail just the downstream work being triggered
	kubectl -n $(NS) logs -f statefulset/router | grep --line-buffered ALERT

metrics: ## Print the numbers that matter
	@kubectl -n $(NS) port-forward svc/router 9090:9090 >/dev/null 2>&1 & \
	sleep 2; \
	curl -s localhost:9090/metrics | grep -E '^jsr_' | grep -vE '^#|_bucket|_sum|_count' | sort; \
	kill %1 2>/dev/null || true

congest: ## Watch one route shed while the others keep draining
	@echo "Shrinking the engagement buffer to 1 so it sheds under real load."
	kubectl -n $(NS) get configmap router-config -o jsonpath='{.data.config\.json}' \
	  | sed 's/"buffer": 32768/"buffer": 1/' > /tmp/jsr-congest.json
	kubectl -n $(NS) create configmap router-config \
	  --from-file=config.json=/tmp/jsr-congest.json --dry-run=client -o yaml \
	  | kubectl -n $(NS) apply -f -
	kubectl -n $(NS) rollout restart statefulset/router
	kubectl -n $(NS) rollout status statefulset/router --timeout=180s
	@echo
	@echo "Now run 'make metrics' and compare:"
	@echo "  jsr_events_dropped_total{route=\"engagement\"}   rising"
	@echo "  jsr_events_handled_total{route=\"content\"}      still rising"
	@echo "Restore with: kubectl apply -f deploy/k8s.yaml && make deploy"

down: ## Delete the namespace, keep the cluster
	kubectl delete namespace $(NS) --ignore-not-found

clean: ## Delete the cluster
	kind delete cluster --name $(CLUSTER)
