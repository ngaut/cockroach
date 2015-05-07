#!/bin/bash

tag="cockroachdb/builder"

function init() {
    docker build --tag="${tag}" - <<EOF
FROM golang:1.4.2

RUN apt-get update -y && \
 apt-get dist-upgrade -y && \
 apt-get install --no-install-recommends --auto-remove -y git build-essential file && \
 apt-get clean autoclean && \
 apt-get autoremove -y && \
 rm -rf /tmp/* /var/lib/{apt,dpkg,cache,log}

CMD ["/bin/bash"]
EOF
}

function build() {
    local gopath="${GOPATH%%:*}"
    local dir="${PWD#${GOPATH}}"
    # Run our build container with a set of volumes mounted that will
    # allow the container to store persistent build data on the host
    # computer.
    docker run -it --rm \
	   --volume="${gopath}/src:/go/src" \
	   --volume="${gopath}/pkg:/go/pkg" \
	   --volume="${gopath}/pkg/linux_amd64_netgo:/usr/src/go/pkg/linux_amd64_netgo" \
	   --volume="${gopath}/bin/linux_amd64:/go/bin" \
	   --workdir="/go${dir}" \
	   "${tag}" "$@"
}

if ! docker images | grep -q "${tag}"; then
    init
fi

build "$@"
