#
# Makefile
# dev, 2018-04-17 15:05
#

all: build

build:
	go build -o bin/tidis-server cmd/server/*

# vim:ft=make
#
