#!/bin/bash

dir=$(mktemp -d .mkimage.XXXXXX)
trap "rm -fr ${dir}" 0

base="$(basename $1)"

cat >> ${dir}/Dockerfile <<EOF
FROM busybox
COPY ./${base} .
ENTRYPOINT ["./${base}"]
EOF

ln "$1" "${dir}/${base}"

docker build --tag="dev/${base}" "${dir}"
