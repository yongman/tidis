#
# Makefile
# yongman, 2018-04-17 15:05
#

all: build

build:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GO111MODULE=on go build -gcflags "all=-N -l" -o bin/tidis-server cmd/server/*

# vim:ft=make
#
