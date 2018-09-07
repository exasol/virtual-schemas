#!/usr/bin/env bash
# This script executes integration tests as defined in
# integration-test-travis.yaml (currently only Exasol integration tests).
#
# An Exasol instance is run using the exasol/docker-db image. Therefore, a
# working installation of Docker and sudo privileges are required.

set -eux
cd "$(dirname "$0")/.."

readonly config="$(pwd)/integration-test-data/integration-test-travis.yaml"
readonly exasol_version=6.0.10
readonly tmp="$(pwd)/integration-test-data/exa'

function cleanup() {
    docker rm -f exasoldb || true
    sudo rm -rf "$tmp" || true
}
trap cleanup EXIT

# Setup directory "exa" with pre-configured EXAConf to attach it to the exasoldb docker container
mkdir -p "$tmp"/{etc,data/storage}
cp integration-test-data/EXAConf integration-test-data/exa/etc/EXAConf
dd if=/dev/zero of=integration-test-data/exa/data/storage/dev.1.data bs=1 count=1 seek=4G
touch integration-test-data/exa/data/storage/dev.1.meta

docker pull exasol/docker-db:latest
docker run \
    --name exasoldb \
    -p 8899:8888 \
    -p 6594:6583 \
    --detach \
    --privileged \
    -v "$tmp:/exa" \
    "exasol/docker-db:$exasol_version" \
    init-sc --node-id 11

docker logs -f exasoldb &

# Wait until database is ready
(docker logs -f --tail 0 exasoldb &) 2>&1 | grep -q -i 'stage4: All stages finished'
sleep 30

mvn -q clean package

# Load virtualschema-jdbc-adapter JAR into BucketFS and wait until it's available.
mvn -q pre-integration-test -DskipTests -Pit -Dintegrationtest.configfile="$config"
(docker exec exasoldb sh -c 'tail -f -n +0 /exa/logs/cored/*bucket*' &) | \
    grep -q -i 'File.*virtualschema-jdbc-adapter.*linked'

mvn -q verify -Pit -Dintegrationtest.configfile="$config" -Dintegrationtest.skipTestSetup=true
