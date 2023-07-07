#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

readonly pk_mode="${1-verify}";
readonly version="2.9.9"

readonly pk_jar="$HOME/.m2/repository/com/exasol/project-keeper-cli/$version/project-keeper-cli-$version.jar"

if [ ! -f "$pk_jar" ]; then
    echo "Downloading Project Keeper $version"
    mvn --batch-mode org.apache.maven.plugins:maven-dependency-plugin:3.3.0:get -Dartifact=com.exasol:project-keeper-cli:$version
fi

echo "Running Project Keeper $version with mode $pk_mode from $pk_jar"
java -jar "$pk_jar" "$pk_mode"
