CERTS_DIR ?= certs
CA_KEYS := $(CERTS_DIR)/ca.pem $(CERTS_DIR)/ca-key.pem $(CERTS_DIR)/ca.csr
SERVER_KEYS := $(CERTS_DIR)/server.crt $(CERTS_DIR)/server.key $(CERTS_DIR)/server.csr
RUN_ARGS ?=

.PHONY: run-eckd
run-eckd: $(SERVER_KEYS)
	nix run .#eckd -- --cert-file $(CERTS_DIR)/server.crt --key-file $(CERTS_DIR)/server.key $(RUN_ARGS) --listen-client-urls 'https://localhost:2379' --advertise-client-urls 'https://localhost:2379'

.PHONY: run-etcd
run-etcd: $(SERVER_KEYS)
	etcd --cert-file $(CERTS_DIR)/server.crt --key-file $(CERTS_DIR)/server.key $(RUN_ARGS) --listen-client-urls 'https://localhost:2379' --advertise-client-urls 'https://localhost:2379'

.PHONY: bench
bench: $(SERVER_KEYS)
	nix run .\#etcd-benchmark -- --endpoints "https://localhost:2379" --cacert $(CERTS_DIR)/ca.pem put key

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

.PHONY: docker-image
docker-image:
	nix build .\#eckd-docker

.PHONY: docker-load
docker-load: docker-image
	docker load -i result

.PHONY: docker-push
docker-push: docker-load
	docker push jeffas/etcd:latest

.PHONY: kind
kind:
	kind delete cluster
	kind create cluster --config kind-config.yaml --image kindest/node:v1.19.1 --retain
