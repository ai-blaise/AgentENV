PROTOC    ?= protoc
GO        ?= go
GOBIN_DIR := $(shell $(GO) env GOPATH)/bin
# scheduler.proto is the node-facing contract and is also compiled into the Rust
# client by the workspace's build.rs. node_stream.proto is spoken between
# scheduler replicas only, so it stays out of that client.
PROTO_SRC := api/proto/scheduler.proto api/proto/node_stream.proto

.PHONY: services proto tidy build test bench bench-services fmt fmt-check vet run-scheduler run-gateway

proto:
	PATH="$(GOBIN_DIR):$$PATH" $(PROTOC) \
		--go_out=paths=source_relative:. \
		--go-grpc_out=paths=source_relative:. \
		$(PROTO_SRC)

tidy:
	$(GO) mod tidy

build:
	$(MAKE) -C gateway build
	$(MAKE) -C scheduler build

test:
	$(MAKE) -C gateway test
	$(MAKE) -C scheduler test

# The scheduler's fleet benchmarks: LookupNode at a million bindings, heartbeat
# reconcile at 10k nodes x 500 sandboxes, placement per strategy. They measure
# and never assert, so they are not part of `test`; the invariants they rest on
# are ordinary tests in the same files and run with everything else. Redis
# leaves need a redis-server (REDIS_SERVER_BIN, or one on PATH) and skip
# without one. The fleet shape is overridable through the -fleet.* flags:
#
#	make bench-services BENCH_FLAGS='-fleet.nodes=1000 -fleet.churn=0.02'
#
# Recorded numbers and what each knob means: docs/src/internals/scheduler-benchmarks.md.
BENCH       ?= BenchmarkLookupNode_1MBindings|BenchmarkHeartbeatReconcile_10kNodes_500Sandboxes|BenchmarkScheduleFleet
BENCH_TIME  ?= 1s
BENCH_FLAGS ?=

bench-services:
	$(GO) test ./scheduler/internal/ -run XXX -bench '$(BENCH)' -benchmem -benchtime=$(BENCH_TIME) -timeout=60m $(BENCH_FLAGS)

bench: bench-services

fmt:
	$(MAKE) -C gateway fmt
	$(MAKE) -C scheduler fmt

fmt-check:
	$(MAKE) -C gateway fmt-check
	$(MAKE) -C scheduler fmt-check

vet:
	$(MAKE) -C gateway vet
	$(MAKE) -C scheduler vet

run-scheduler:
	$(GO) run ./scheduler/cmd -config ./config/local.json

run-gateway:
	$(GO) run ./gateway/cmd -config ./config/local.json
