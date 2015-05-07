#!/bin/bash

cd $(dirname $0)/..
run/build.sh make STATIC=1 COCKROACH=cockroach.linux build
trap "rm -f ./cockroach.linux" 0
run/mkimage.sh ./cockroach.linux
go run run/local-cluster.go -i dev/cockroach.linux -c
