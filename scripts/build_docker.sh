#!/bin/bash

set -x
# Find the directory we exist within
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd ${DIR}

VERSION=`git describe --always`

mkdir build
cp ../build/eventtank build/
cp ./etc/eventtank.ini build/

docker build -t raintank/eventtank:$VERSION .
docker tag raintank/eventtank:$VERSION raintank/eventtank:latest
