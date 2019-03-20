#!/usr/bin/env bash

set -xe

SCALA_VERSION=2.12
KAFKA_VERSION=2.1.1
KAFKA_APP="kafka_${SCALA_VERSION}-${KAFKA_VERSION}"
TARBALL="${KAFKA_APP}.tgz"
CHECKSUMS="${TARBALL}.sha512"
KAFKA_ROOT_DIR=swh/journal/tests
KAFKA_DIR="${KAFKA_ROOT_DIR}/${KAFKA_APP}"
KAFKA_LINK="${KAFKA_ROOT_DIR}/kafka"
URL="http://apache.mirrors.ovh.net/ftp.apache.org/dist/kafka/${KAFKA_VERSION}/${TARBALL}"
KAFKA_CHECKSUM=a2e8168e8de6b45e8fca1f2883f0744d3c5a939b70d8a47a5428b72188501d4c2fc11bc35759f2392680d4e8ecf2fa9d0e518e77fd28393afba22194ad018b10

case $1 in
    "install")
	echo "Kafka installation started."

        if [ ! -f $TARBALL ]; then
          echo "Kafka download"
          wget $URL
          echo "${KAFKA_CHECKSUM}  ${TARBALL}" > $CHECKSUMS
          sha512sum -c $CHECKSUMS

          if [ $? -ne 0 ]; then
            echo "Kafka download: failed to retrieve ${TARBALL}";
            exit 1
          fi
          echo "Kafka download done"
        fi

        if [ ! -d $KAFKA_DIR ]; then
          echo "Kafka extraction"

          tar xvf $TARBALL -C $KAFKA_ROOT_DIR
          pushd $KAFKA_ROOT_DIR && ln -nsf $KAFKA_APP kafka && popd
          echo "Kafka extraction done"
        fi
	echo "Kafka installation done. Kafka is installed at $KAFKA_DIR"
	;;
    "clean")
	echo "Kafka cleanup started."
	[ -d $KAFKA_DIR ] && rm -rf $KAFKA_DIR
	[ -L $KAFKA_LINK ] && rm $KAFKA_LINK
	echo "Kafka cleanup done."
	;;
    *)
	echo "Unknown command, do nothing"
	exit 1;
esac
