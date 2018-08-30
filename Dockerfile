FROM golang:1.10.1-alpine as builder

RUN apk add --no-cache make git

COPY . /go/src/github.com/yongman/tidis

WORKDIR /go/src/github.com/yongman/tidis/

RUN make

RUN ls -al /go/src/github.com/yongman/tidis/bin/tidis-server

RUN mv /go/src/github.com/yongman/tidis/bin/tidis-server /tidis-server

WORKDIR /

EXPOSE 5379

ENTRYPOINT ["/tidis-server"]

