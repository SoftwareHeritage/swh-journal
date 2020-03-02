#!/usr/bin/env bash

set -xe

SCALA_VERSION=2.12
KAFKA_VERSION=2.4.0
KAFKA_CHECKSUM=53b52f86ea56c9fac62046524f03f75665a089ea2dae554aefe3a3d2694f2da88b5ba8725d8be55f198ba80695443559ed9de7c0b2a2817f7a6141008ff79f49

KAFKA_APP="kafka_${SCALA_VERSION}-${KAFKA_VERSION}"
TARBALL="${KAFKA_APP}.tgz"
URL="http://apache.mirrors.ovh.net/ftp.apache.org/dist/kafka/${KAFKA_VERSION}/${TARBALL}"
CHECKSUMS="${TARBALL}.sha512"
KAFKA_ROOT_DIR=swh/journal/tests
KAFKA_DIR="${KAFKA_ROOT_DIR}/${KAFKA_APP}"
KAFKA_LINK="${KAFKA_ROOT_DIR}/kafka"

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
    "clean-cache")
        echo "Kafka cleanup cache started."
        [ -f $TARBALL ] && rm $TARBALL
        [ -f $CHECKSUMS ] && rm $CHECKSUMS
        echo "Kafka cleanup cache done."
        ;;
    *)
        echo "Unknown command, do nothing"
        exit 1;
esac
