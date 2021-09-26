TRACE_FILE ?= trace.requests
CERTS_DIR ?= certs
CA_KEYS := $(CERTS_DIR)/ca.pem $(CERTS_DIR)/ca-key.pem $(CERTS_DIR)/ca.csr
SERVER_KEYS := $(CERTS_DIR)/server.crt $(CERTS_DIR)/server.key $(CERTS_DIR)/server.csr
RUN_ARGS ?=
DOT_FILES := $(shell find -name '*.dot')
SVG_FILES := $(patsubst %.dot, %.svg, $(DOT_FILES))

ETCD_IMAGE := quay.io/coreos/etcd:v3.4.13
ECETCD_IMAGE := jeffas/etcd:latest
ECKD_IMAGE := jeffas/eckd:latest
RECETCD_IMAGE := jeffas/recetcd:latest
BENCHER_IMAGE := jeffas/bencher:latest

.PHONY: run-eckd
run-eckd: $(SERVER_KEYS)
	rm -rf default.eckd
	nix run .#eckd -- --cert-file $(CERTS_DIR)/server.crt --key-file $(CERTS_DIR)/server.key $(RUN_ARGS) --listen-client-urls 'https://localhost:2379' --advertise-client-urls 'https://localhost:2379'

.PHONY: run-eckd-sync
run-eckd-sync: $(SERVER_KEYS)
	rm -rf default.eckd
	nix run .#eckd -- --cert-file $(CERTS_DIR)/server.crt --key-file $(CERTS_DIR)/server.key $(RUN_ARGS) --listen-client-urls 'https://localhost:2379' --advertise-client-urls 'https://localhost:2379' --sync

.PHONY: run-recetcd
run-recetcd: $(SERVER_KEYS)
	rm -rf default.recetcd
	nix run .#recetcd -- --cert-file $(CERTS_DIR)/server.crt --key-file $(CERTS_DIR)/server.key $(RUN_ARGS) --listen-client-urls 'https://localhost:2379' --advertise-client-urls 'https://localhost:2379'

.PHONY: run-etcd
run-etcd: $(SERVER_KEYS)
	rm -rf default.etcd
	etcd --cert-file $(CERTS_DIR)/server.crt --key-file $(CERTS_DIR)/server.key $(RUN_ARGS) --listen-client-urls 'https://localhost:2379' --advertise-client-urls 'https://localhost:2379' --listen-metrics-urls 'http://localhost:2381'

.PHONY: bench
bench: $(SERVER_KEYS)
	nix run .\#etcd-benchmark -- --endpoints "https://localhost:2379" --cacert $(CERTS_DIR)/ca.pem put key

.PHONY: bencher
bencher: $(SERVER_KEYS)
	nix run .\#bencher -- --endpoints "https://localhost:2379" --cacert $(CERTS_DIR)/ca.pem bench put-range

.PHONY: run-trace
run-trace: $(SERVER_KEYS)
	nix run .\#bencher -- --endpoints "https://localhost:2379" --cacert $(CERTS_DIR)/ca.pem trace --in-file $(TRACE_FILE)

.PHONY: get-trace
get-trace:
	kubectl -n kube-system cp etcd-kind-control-plane:/tmp/trace.out $(TRACE_FILE)

$(CA_KEYS): $(CERTS_DIR)/ca-csr.json
	cfssl gencert -initca $(CERTS_DIR)/ca-csr.json | cfssljson -bare $(CERTS_DIR)/ca -

$(SERVER_KEYS): $(CA_KEYS) $(CERTS_DIR)/ca-config.json $(CERTS_DIR)/server.json
	cfssl gencert -ca=$(CERTS_DIR)/ca.pem -ca-key=$(CERTS_DIR)/ca-key.pem -config=$(CERTS_DIR)/ca-config.json -profile=server $(CERTS_DIR)/server.json | cfssljson -bare $(CERTS_DIR)/server -
	mv $(CERTS_DIR)/server.pem $(CERTS_DIR)/server.crt
	mv $(CERTS_DIR)/server-key.pem $(CERTS_DIR)/server.key

.PHONY: clean
clean:
	rm -f $(CA_KEYS) $(SERVER_KEYS)
	rm -rf default.{eckd,etcd}
	rm -f result result-lib
	cargo clean

.PHONY: docker-eckd
docker-eckd:
	nix build .\#recetcd-docker-etcd
	docker load -i result
	nix build .\#eckd-docker
	docker load -i result

.PHONY: docker-recetcd
docker-recetcd:
	nix build .\#recetcd-docker
	docker load -i result

.PHONY: docker-bencher
docker-bencher:
	nix build .\#bencher-docker
	docker load -i result

.PHONY: docker-load
docker-load: docker-eckd docker-recetcd docker-bencher

.PHONY: docker-push
docker-push: docker-load
	docker push $(ECETCD_IMAGE)
	docker push $(ECKD_IMAGE)
	docker push $(RECETCD_IMAGE)
	docker push $(BENCHER_IMAGE)

.PHONY: test
test: docker-recetcd
	docker rm -f recetcd etcd
	docker run --name recetcd --network host -d $(RECETCD_IMAGE) --advertise-client-urls http://127.0.0.1:2389 --listen-peer-urls http://127.0.0.1:2390 --listen-metrics-urls http://127.0.0.1:1291 --persister sled
	docker run --name etcd --network host -d $(ETCD_IMAGE) etcd --advertise-client-urls http://127.0.0.1:2379
	sleep 3
	cargo test --workspace --exclude kubernetes-proto -- --test-threads=1
	docker rm -f recetcd etcd

.PHONY: kind
kind:
	kind delete cluster
	kind create cluster --config kind-config.yaml --image kindest/node:v1.20.7 --retain || sleep 5 && docker cp kind-control-plane:/var/log/pods logs

.PHONY: diagrams
diagrams: $(SVG_FILES)

%.svg: %.dot
	dot -Tsvg $< > $@

.PHONY: protos
protos:
	rm -rf kubernetes-proto/proto
	rsync -avm --include='*.proto' --filter 'hide,! */' ../kubernetes/staging/src/ kubernetes-proto/proto

.PHONY: jaeger
jaeger:
	docker run --name jaeger -d -p6831:6831/udp -p6832:6832/udp -p16686:16686 jaegertracing/all-in-one:latest

.PHONY: build-musl
build-musl:
	# only supports building libraries that don't use openssl
	cargo build --release --target x86_64-unknown-linux-musl --bin bencher-experiment

.PHONY: copy-to-binky
copy-to-binky: build-musl
	rsync -vv --progress target/x86_64-unknown-linux-musl/release/bencher-experiment binky:/home/apj39/eckd-rs/bencher-experiment

.PHONY: copy-from-binky
copy-from-binky:
	rsync -r -vv --progress binky:/local/scratch/apj39/bencher-experiment/results/ experiments/bencher/results/
