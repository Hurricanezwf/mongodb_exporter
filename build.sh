#!/bin/bash

set -e

export CGO_ENABLED=false

rm -rf ./bin/
mkdir -p ./bin/
GOOS=linux  GOARCH=amd64 go build -ldflags "-s -w" -o ./bin/mongodb-exporter-v0.41-0.0.1-linux-amd64
GOOS=darwin GOARCH=arm64 go build -ldflags "-s -w" -o ./bin/mongodb-exporter-v0.41-0.0.1-darwin-arm64
