#!/usr/bin/env bash
# This script executes integration tests as defined in
# integration-test-travis.yaml (currently only Exasol integration tests).
#
# An Exasol instance is run using the exasol/docker-db image. Therefore, a
# working installation of Docker and sudo privileges are required.

set -eux
cd "$(dirname "$0")/.."

readonly config="$(pwd)/integration-test-data/integration-test-travis.yaml"
readonly exasol_docker_image_version="6.1.2-d1"
readonly docker_image="exasol/docker-db:$exasol_docker_image_version"
readonly docker_name="exasoldb"
readonly jdbc_driver_dir="$(pwd)/integration-test-data/drivers"
readonly docker_helper="$(pwd)/integration-test-data/socker.py"
readonly tmp="$(mktemp -td exasol-vs-adapter-integration.XXXXXX)" || exit 1
readonly config_with_ips="$(pwd)/integration-test-data/integration-test-travis_with_ips.yaml"
readonly exaconf="$tmp/cluster/EXAConf"
readonly docker_db_repository="https://github.com/EXASOL/docker-db.git"

function cleanup() {
    cleanup_remote_dbs || true
	cleanup_docker "$tmp" || true
    sudo rm -rf "$tmp" || true
}
trap cleanup EXIT

main() {
	prepare_docker "$tmp"
	init_docker "$tmp"
	check_docker_ready
	build
	deploy_jdbc_drivers
	start_remote_dbs
	replace_hosts_with_ips_in_config
	run_tests
}

deploy_jdbc_drivers() {
	bucket_fs_url="$(awk '/bucketFsUrl/{print $NF}' $config)"
	bfs_url_no_http="$(echo $bucket_fs_url | awk -F/ '{for(i=3;i<=NF;++i)printf "%s/",$i}')"
	bucket_fs_pwd="$(awk '/WritePasswd/{ print $3; }' $exaconf | base64 -d)"
	bucket_fs_upload_url="http://w:$bucket_fs_pwd@$bfs_url_no_http/"
	#upload drivers that are part of the repository
	for d in "$jdbc_driver_dir"/*
	do
	    db_driver="$(basename $d)"
	    find "$jdbc_driver_dir/$db_driver" -type f -exec curl -X PUT -T {} "$bucket_fs_upload_url/drivers/jdbc/$db_driver/" \;
	done
	#upload additional (local) drivers
	additional_jdbc_driver_dir="$(awk '/additionalJDBCDriverDir/{print $NF}' $config)"
	if [ -d "$additional_jdbc_driver_dir" ]; then
	    for d in "$additional_jdbc_driver_dir"/*
	    do
	        db_driver="$(basename $d)"
	        find "$additional_jdbc_driver_dir/$db_driver" -type f -exec curl -X PUT -T {} "$bucket_fs_upload_url/drivers/jdbc/$db_driver/" \;
	    done
	fi
	#deploy oracle instantclient
	instantclient_dir="$(awk '/instantclientDir/{print $NF}' $config)"
	instantclient_path="$instantclient_dir/instantclient-basic-linux.x64-12.1.0.2.0.zip"
	if [ -f "$instantclient_path" ]; then
	    curl -X PUT -T "$instantclient_path" "$bucket_fs_upload_url/drivers/oracle/"
	fi
	#deploy adapter jar
	adapter_jar="$(awk '/jdbcAdapterPath/{ n=split($0,a,"/"); print a[n];}' $config)"
	adapter_path="./virtualschema-jdbc-adapter-dist/target/$adapter_jar"
	curl -X PUT -T "$adapter_path" "$bucket_fs_upload_url"
}

replace_hosts_with_ips_in_config() {
	"$docker_helper" --add_docker_hosts "$config" > "$config_with_ips"
}

start_remote_dbs() {
	pushd "$tmp"
	"$docker_helper" --run "$config"
	sleep 10
	popd
}

cleanup_remote_dbs() {
	pushd "$tmp"
	"$docker_helper" --rm "$config"
	popd
}

prepare_docker() {
	docker pull "$docker_image"
	git clone --branch "$exasol_docker_image_version" "$docker_db_repository" "$1/$docker_name"
	pushd "$1/$docker_name"
	pipenv install -r exadt_requirements.txt
	pipenv run ./exadt create-cluster --root "$1"/cluster --create-root "$docker_name"
	pipenv run ./exadt init-cluster --image "$docker_image" --license ./license/license.xml --auto-storage "$docker_name"
	popd
}

init_docker() {
	pushd "$1/$docker_name"
	pipenv run ./exadt start-cluster "$docker_name"
	exa_container_name="$(docker ps --filter ancestor=exasol/docker-db:6.1.2-d1 --format "{{.Names}}")"
	docker logs -f "$exa_container_name" &
	popd
}

check_docker_ready() {
	# Wait until database is ready
	exa_container_name="$(docker ps --filter ancestor=exasol/docker-db:6.1.2-d1 --format "{{.Names}}")"
	(docker logs -f --tail 0 "$exa_container_name" &) 2>&1 | grep -q -i 'stage4: All stages finished'
	sleep 30
}

cleanup_docker() {
	pushd "$1/$docker_name"
	pipenv run ./exadt --yes stop-cluster "$docker_name" || true
	pipenv run ./exadt --yes delete-cluster "$docker_name" || true
	popd
}

build() {
	mvn -q clean package
}

run_tests() {
	mvn -q verify -Pit -Dintegrationtest.configfile="$config_with_ips" -Dintegrationtest.skipTestSetup=true
}

main "$@"