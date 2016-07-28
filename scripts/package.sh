#!/bin/bash
set -x
BASE=$(dirname $0)
CODE_DIR=$(readlink -e "$BASE/../")

BUILD_ROOT=$CODE_DIR/build

ARCH="$(uname -m)"
VERSION=$(git describe --long --always)



## ubuntu 14.04
BUILD=${BUILD_ROOT}/upstart

mkdir -p ${BUILD}/usr/bin
mkdir -p ${BUILD}/etc/init
mkdir -p ${BUILD}/etc/raintank

cp ${BASE}/etc/eventtank.ini ${BUILD}/etc/raintank/
cp ${BUILD}/eventtank ${BUILD}/usr/bin/

PACKAGE_NAME="${BUILD}/eventtank-${VERSION}_${ARCH}.deb"
fpm -s dir -t deb \
  -v ${VERSION} -n eventtank -a ${ARCH} --description "Persist raintank Events to Elasticsearch" \
  --config-files /etc/raintank/ \
  --deb-upstart ${BASE}/etc/upstart/eventtank.conf \
  -m "Raintank Inc. <hello@raintank.io>" --vendor "raintank.io" \
  --license "Apache2.0" -C ${BUILD} -p ${PACKAGE_NAME} .

## ubuntu 16.04
BUILD=${BUILD_ROOT}/systemd
mkdir -p ${BUILD}/usr/bin
mkdir -p ${BUILD}/lib/systemd/system/
mkdir -p ${BUILD}/etc/raintank

cp ${BASE}/etc/eventtank.ini ${BUILD}/etc/raintank/
cp ${BUILD}/eventtank ${BUILD}/usr/bin/
cp ${BASE}/etc/systemd/eventtank.service ${BUILD}/lib/systemd/system/

PACKAGE_NAME="${BUILD}/eventtank-${VERSION}_${ARCH}.deb"
fpm -s dir -t deb \
  -v ${VERSION} -n eventtank -a ${ARCH} --description "Persist raintank Events to Elasticsearch" \
  --config-files /etc/raintank/ \
  -m "Raintank Inc. <hello@raintank.io>" --vendor "raintank.io" \
  --license "Apache2.0" -C ${BUILD} -p ${PACKAGE_NAME} .