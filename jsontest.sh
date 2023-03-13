#!/usr/bin/env sh

./grpcurl -import-path proto/mergeable-proto/proto -proto rpc.proto list
./grpcurl -import-path proto/mergeable-proto/proto -proto rpc.proto describe etcdserverpb.PutRequest

key=$(echo -n "foo" | base64)
value=$(echo -n "{}" | base64)
./grpcurl -import-path proto/mergeable-proto/proto -proto rpc.proto -d "{\"key\":\"$key\",\"value\":\"$value\"}" -plaintext localhost:2379 etcdserverpb.KV/Put

./grpcurl -import-path proto/mergeable-proto/proto -proto rpc.proto -d "{\"key\":\"$key\"}" -plaintext localhost:2379 etcdserverpb.KV/Range
