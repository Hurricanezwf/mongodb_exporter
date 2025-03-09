#!/bin/bash

set -e

rm -rf ./bin/
mkdir -p ./bin/
GOOS=linux  GOARCH=amd64 go build -ldflags "-s -w" -o ./bin/mongodb-exporter-v0.41-0.0.1-linux-amd64
