TRACE_FILE ?= trace.requests
CERTS_DIR ?= certs
CA_KEYS := $(CERTS_DIR)/ca.pem $(CERTS_DIR)/ca-key.pem $(CERTS_DIR)/ca.csr
SERVER_KEYS := $(CERTS_DIR)/server.crt $(CERTS_DIR)/server.key $(CERTS_DIR)/server.csr
PEER_KEYS := $(CERTS_DIR)/peer.crt $(CERTS_DIR)/peer.key $(CERTS_DIR)/peer.csr
RUN_ARGS ?=
DOT_FILES := $(shell find -name '*.dot')
SVG_FILES := $(patsubst %.dot, %.svg, $(DOT_FILES))

ETCD_IMAGE := jeffas/etcd:v3.4.14
DISMERGE_IMAGE := jeffas/dismerge:latest
MERGEABLE_ETCD_IMAGE := jeffas/mergeable-etcd:latest
BENCHER_IMAGE := jeffas/bencher:latest

.PHONY: run-mergeable-etcd
run-mergeable-etcd: $(SERVER_KEYS)
	rm -rf default.mergeable-etcd
	nix run .#mergeable-etcd -- $(RUN_ARGS) --listen-client-urls 'http://127.0.0.1:2379' --advertise-client-urls 'http://127.0.0.1:2379'

.PHONY: run-etcd
run-etcd: $(SERVER_KEYS)
	rm -rf default.etcd
	etcd $(RUN_ARGS) --listen-client-urls 'http://127.0.0.1:2379' --advertise-client-urls 'http://127.0.0.1:2379' --listen-metrics-urls 'http://127.0.0.1:2381'

.PHONY: bench
bench: $(SERVER_KEYS)
	time nix run .\#etcd-benchmark -- --endpoints "http://127.0.0.1:2379" --conns 100 --clients 1000 put --total 100000

.PHONY: bencher
bencher: $(SERVER_KEYS)
	time nix run .\#bencher -- --endpoints "http://127.0.0.1:2379" --total 100000 bench put-random 100 --interval 100000

.PHONY: run-trace
run-trace: $(SERVER_KEYS)
	nix run .\#bencher -- --endpoints "https://127.0.0.1:2379" --cacert $(CERTS_DIR)/ca.pem trace --in-file $(TRACE_FILE)

.PHONY: get-trace
get-trace:
	kubectl -n kube-system cp etcd-kind-control-plane:/tmp/trace.out $(TRACE_FILE)

$(CA_KEYS): $(CERTS_DIR)/ca-csr.json
	cfssl gencert -initca $(CERTS_DIR)/ca-csr.json | cfssljson -bare $(CERTS_DIR)/ca -

$(SERVER_KEYS): $(CA_KEYS) $(CERTS_DIR)/ca-config.json $(CERTS_DIR)/server.json
	cfssl gencert -ca=$(CERTS_DIR)/ca.pem -ca-key=$(CERTS_DIR)/ca-key.pem -config=$(CERTS_DIR)/ca-config.json -profile=server $(CERTS_DIR)/server.json | cfssljson -bare $(CERTS_DIR)/server -
	mv $(CERTS_DIR)/server.pem $(CERTS_DIR)/server.crt
	mv $(CERTS_DIR)/server-key.pem $(CERTS_DIR)/server.key

$(PEER_KEYS): $(CA_KEYS) $(CERTS_DIR)/ca-config.json $(CERTS_DIR)/peer.json
	cfssl gencert -ca=$(CERTS_DIR)/ca.pem -ca-key=$(CERTS_DIR)/ca-key.pem -config=$(CERTS_DIR)/ca-config.json -profile=server $(CERTS_DIR)/peer.json | cfssljson -bare $(CERTS_DIR)/peer -
	mv $(CERTS_DIR)/peer.pem $(CERTS_DIR)/peer.crt
	mv $(CERTS_DIR)/peer-key.pem $(CERTS_DIR)/peer.key

.PHONY: clean
clean:
	rm -f $(CA_KEYS) $(SERVER_KEYS) $(PEER_KEYS)
	rm -rf default.*
	rm -f result result-lib
	cargo clean

.PHONY: docker-mergeable-etcd
docker-mergeable-etcd:
	nix build .\#mergeable-etcd-docker
	docker load -i result

.PHONY: docker-dismerge
docker-dismerge:
	nix build .\#dismerge-docker
	docker load -i result

.PHONY: docker-etcd
docker-etcd:
	nix build .\#etcd-docker
	docker load -i result

.PHONY: docker-bencher
docker-bencher:
	nix build .\#bencher-docker
	docker load -i result

.PHONY: docker-load
docker-load: docker-mergeable-etcd docker-dismerge docker-etcd docker-bencher

.PHONY: docker-push
docker-push: docker-load
	docker push $(MERGEABLE_ETCD_IMAGE)
	docker push $(DISMERGE_IMAGE)
	docker push $(ETCD_IMAGE)
	docker push $(BENCHER_IMAGE)

.PHONY: test
test:
	./test.sh

.PHONY: kind
kind:
	rm -rf logs
	kind delete cluster
	kind create cluster -v 10 --config kind-config.yaml --image kindest/node:v1.20.7 --retain || sleep 5 && docker cp kind-control-plane:/var/log/pods logs
	kubectl taint nodes --all node-role.kubernetes.io/master- || true

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
