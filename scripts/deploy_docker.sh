#!/bin/bash

set -x
# Find the directory we exist within
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd ${DIR}

VERSION=`git describe --always`

docker push raintank/tsdb-gw:$VERSION
docker push raintank/tsdb-gw:latest
