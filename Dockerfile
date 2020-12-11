FROM golang:1.15-alpine3.12 as builder

RUN apk add --no-cache make git

COPY . /go/src/github.com/yongman/tidis

WORKDIR /go/src/github.com/yongman/tidis/

RUN make

FROM scratch
COPY --from=builder /go/src/github.com/yongman/tidis/bin/tidis-server /tidis-server

WORKDIR /

EXPOSE 5379

ENTRYPOINT ["/tidis-server"]

