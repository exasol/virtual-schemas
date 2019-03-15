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
readonly jdbc_driver_dir="$(pwd)/integration-test-data/drivers"
readonly docker_helper="$(pwd)/integration-test-data/socker.py"
readonly tmp="$(mktemp -td exasol-vs-adapter-integration.XXXXXX)" || exit 1
readonly config_with_ips="$(pwd)/integration-test-data/integration-test-travis_with_ips.yaml"

function cleanup() {
    docker rm -f exasoldb || true
    sudo rm -rf "$tmp" || true
	cleanup_remote_dbs || true
}
trap cleanup EXIT

main() {
	prepare_configuration_dir "$tmp/etc"
	prepare_data_dir "$tmp/data/storage"
	init_docker
	check_docker_ready
	build
	upload_jar_to_bucket
	deploy_jdbc_drivers
	start_remote_dbs
	replace_hosts_with_ips_in_config
	run_tests
}

deploy_jdbc_drivers() {
	bucket_fs_url=$(awk '/bucketFsUrl/{print $NF}' $config)
	bfs_url_no_http=$(echo $bucket_fs_url | awk -F/ '{for(i=3;i<=NF;++i)printf "%s/",$i}')
	bucket_fs_pwd=$(awk '/bucketFsPassword/{print $NF}' $config)
	bucket_fs_upload_url=http://w:$bucket_fs_pwd@$bfs_url_no_http/drivers/
	for d in $jdbc_driver_dir/*
	do
	    db_driver=$(basename $d)
	    find $jdbc_driver_dir/$db_driver -type f -exec curl -X PUT -T {} $bucket_fs_upload_url/jdbc/$db_driver/ \;
	done
	#deploy oracle instantclient
	instantclient_dir=$(awk '/instantclientDir/{print $NF}' $config)
	instantclient_path=$instantclient_dir/instantclient-basic-linux.x64-12.1.0.2.0.zip
	if [ -f $instantclient_path ]; then
	    curl -X PUT -T $instantclient_path $bucket_fs_upload_url/drivers/oracle/
	fi
	#workaround for https://github.com/exasol/docker-db/issues/26
	docker exec -d exasoldb mkdir -p /exa/data/bucketfs/default/drivers
	docker exec -d exasoldb ln -s /exa/data/bucketfs/bfsdefault/.dest/default/drivers/jdbc /exa/data/bucketfs/default/drivers/jdbc
}

replace_hosts_with_ips_in_config() {
	$docker_helper --add_docker_hosts $config > $config_with_ips
}

start_remote_dbs() {
	$docker_helper --run $config
	sleep 10
}

cleanup_remote_dbs() {
	$docker_helper --rm $config
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
	mvn -q verify -Pit -Dintegrationtest.configfile="$config_with_ips" -Dintegrationtest.skipTestSetup=true
}

main "$@"