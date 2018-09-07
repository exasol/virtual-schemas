#!/usr/bin/env bash
# This script executes integration tests as defined in
# integration-test-travis.yaml (currently only Exasol integration tests).
#
# An Exasol instance is run using the exasol/docker-db image. Therefore, a
# working installation of Docker and sudo privileges are required.

set -eux
cd "$(dirname "$0")/.."

readonly config="$(pwd)/integration-test-data/integration-test-travis.yaml"
readonly exasol_docker_image_version="6.0.10-d1"
readonly docker_image="exasol/docker-db:$exasol_docker_image_version"
readonly docker_name="exasoldb"
readonly tmp="$(mktemp -td exasol-vs-adapter-integration.XXXXXX)" || exit 1

function cleanup() {
    docker rm -f exasoldb || true
    sudo rm -rf "$tmp" || true
}
trap cleanup EXIT

# Setup directory "exa" with pre-configured EXAConf to attach it to the exasoldb docker container
mkdir -p "$tmp"/{etc,data/storage}
cp integration-test-data/EXAConf "$tmp/etc/EXAConf"
dd if=/dev/zero of="$tmp/data/storage/dev.1.data" bs=1 count=1 seek=4G
touch "$tmp/data/storage/dev.1.meta"

docker pull "$docker_image"
docker run \
    --name "$docker_name" \
    -p 8899:8888 \
    -p 6594:6583 \
    --detach \
    --privileged \
    -v "$tmp:/exa" \
    "$docker_image" \
    init-sc --node-id 11

docker logs -f "$docker_name" &

# Wait until database is ready
(docker logs -f --tail 0 "$docker_name" &) 2>&1 | grep -q -i 'stage4: All stages finished'
sleep 30

mvn -q clean package

# Load virtualschema-jdbc-adapter JAR into BucketFS and wait until it's available.
mvn -q pre-integration-test -DskipTests -Pit -Dintegrationtest.configfile="$config"
(docker exec "$docker_name" sh -c 'tail -f -n +0 /exa/logs/cored/*bucket*' &) | \
    grep -q -i 'File.*virtualschema-jdbc-adapter.*linked'

mvn -q verify -Pit -Dintegrationtest.configfile="$config" -Dintegrationtest.skipTestSetup=true
