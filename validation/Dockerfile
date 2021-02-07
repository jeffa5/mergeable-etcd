FROM golang:1.15.6-alpine3.12 as builder

RUN apk add git

RUN git clone https://github.com/jeffa5/etcd.git /etcd && \
    cd /etcd/tools/benchmark && \
    git checkout benchmark-no-bar && \
    go build && \
    go install

FROM alpine:3.12

COPY --from=builder /go/bin/benchmark /usr/local/bin/

ENTRYPOINT ["benchmark"]
