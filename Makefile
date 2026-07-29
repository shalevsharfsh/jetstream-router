# Targets mirror the README exactly. If the two disagree, the README is right and
# this file is the defect — it is what `make deploy` claims to automate.
CLUSTER ?= jetstream-router
IMAGE   ?= jetstream-router:dev
APP     ?= jetstream-router
PORT    ?= 8080

.DEFAULT_GOAL := help
.PHONY: help build test race lint run image cluster deploy logs alerts metrics congest restore down clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

build: ## Compile the binary
	go build -o bin/router ./cmd/router

test: ## Run the tests
	go test ./...

race: ## Run the tests with the race detector
	go test -race ./...

lint: ## go vet, plus a gofmt check that fails rather than reformatting
	go vet ./...
	@test -z "$$(gofmt -l . )" || (echo "gofmt needed:"; gofmt -l .; exit 1)

run: build ## Run locally against the live firehose
	CONFIG_PATH=local.config.json ./bin/router

image: ## Build the container image
	docker build -f deploy/Dockerfile -t $(IMAGE) .

cluster: ## Create the kind cluster (idempotent)
	@kind get clusters | grep -qx $(CLUSTER) || kind create cluster --name $(CLUSTER)

deploy: cluster image ## README steps 1-3: cluster, build, load, apply
	kind load docker-image $(IMAGE) --name $(CLUSTER)
	kubectl apply -f deploy/
	kubectl rollout status deploy/$(APP) --timeout=180s
	@echo
	@echo "deployed. try:  make alerts"

logs: ## Tail everything
	kubectl logs -f deploy/$(APP)

alerts: ## Tail only the downstream work being triggered
	kubectl logs -f deploy/$(APP) | grep --line-buffered -E 'keyword matched|threshold crossed|burst detected'

metrics: ## Print the per-route counters
	@kubectl port-forward deploy/$(APP) $(PORT):$(PORT) >/dev/null 2>&1 & \
	sleep 3; \
	curl -s localhost:$(PORT)/metrics | grep -E '^(events_|handler_panics_|cursor_lag_|connection_state)' | sort; \
	kill %1 2>/dev/null || true

congest: ## Shrink the engagement buffer to 1 and watch that route shed while the others drain
	kubectl get configmap $(APP) -o jsonpath='{.data.config\.json}' \
	  | sed 's/"buffer": 32768/"buffer": 1/' > /tmp/jsr-congest.json
	kubectl create configmap $(APP) --from-file=config.json=/tmp/jsr-congest.json \
	  --dry-run=client -o yaml | kubectl apply -f -
	kubectl rollout restart deploy/$(APP)
	kubectl rollout status deploy/$(APP) --timeout=180s
	@echo
	@echo "give it a minute, then 'make metrics' and compare:"
	@echo "  events_dropped_total{route=\"engagement\"}   rising"
	@echo "  events_routed_total{route=\"content\"}       still rising"
	@echo "restore with: make restore"

restore: ## Undo make congest
	kubectl apply -f deploy/configmap.yaml
	kubectl rollout restart deploy/$(APP)
	kubectl rollout status deploy/$(APP) --timeout=180s

down: ## Delete the workload, keep the cluster
	kubectl delete -f deploy/ --ignore-not-found

clean: ## Delete the cluster
	kind delete cluster --name $(CLUSTER)
