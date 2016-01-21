#!/bin/bash

usage() {
    echo "Usage: $0 <json>"
}

[ $# -lt 1 ] && { usage; exit 1; }

JSON=$1
VERSION=$(basename $JSON)-$(date '+%s')

(cd ..; sbt "project s2lambda" "+ assembly")

docker build --no-cache --build-arg json=$JSON -t s2lambda:$VERSION .

echo "docker run -it --rm s2lambda:$VERSION"

