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

main() {
	prepare_configuration_dir "$tmp/etc"
	prepare_data_dir "$tmp/data/storage"
	init_docker
	check_docker_ready
	build
	upload_jar_to_bucket
	run_tests
}


prepare_configuration_dir() {
	mkdir -p "$1"
	cp integration-test-data/EXAConf "$1/EXAConf"
}

prepare_data_dir() {
	mkdir -p "$1"
	dd if=/dev/zero of="$1/dev.1.data" bs=1 count=1 seek=4G
	touch "$1/dev.1.meta"
}

init_docker() {
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
}

check_docker_ready() {
	# Wait until database is ready
	(docker logs -f --tail 0 "$docker_name" &) 2>&1 | grep -q -i 'stage4: All stages finished'
	sleep 30
}

build() {
	mvn -q clean package
}

upload_jar_to_bucket() {
	mvn -q pre-integration-test -DskipTests -Pit -Dintegrationtest.configfile="$config"
	(docker exec "$docker_name" sh -c 'tail -f -n +0 /exa/logs/cored/*bucket*' &) | \
	    grep -q -i 'File.*virtualschema-jdbc-adapter.*linked'
}

run_tests() {
	mvn -q verify -Pit -Dintegrationtest.configfile="$config" -Dintegrationtest.skipTestSetup=true
}

main "$@"