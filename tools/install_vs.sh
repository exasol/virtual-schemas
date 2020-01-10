#!/bin/bash
readonly BUCKET_URL='http://w:jars@localhost:2580/jars/'
readonly VS_JAR_SOURCE_PATH="$1"
readonly JAR_FILE=$(basename "$VS_JAR_SOURCE_PATH")
readonly JAR_URL="$BUCKET_URL/$JAR_FILE"

echo "Installing '$JAR_FILE' in bucket '$BUCKET_URL'"

curl -X PUT -T "$VS_JAR_SOURCE_PATH" "$JAR_URL"