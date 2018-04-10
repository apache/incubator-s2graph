#!/usr/bin/env bash
ROOTDIR=`pwd`/..
WORKDIR=`pwd`
PREFIX="[s2graph] "
REST="localhost:8000"

msg() {
    LEVEL=$1
    MSG=$2
    TIME=`date +"%Y-%m-%d %H:%M:%S"`
    echo "${PREFIX} $LEVEL $MSG"
}

q() { 
    MSG=$1
    TIME=`date +"%Y-%m-%d %H:%M:%S"`
    echo ""
    read -r -p "${PREFIX}  >>> $MSG " var
}

info()  { MSG=$1; msg "" "$MSG";}
warn()  { MSG=$1; msg "[WARN] " "$MSG";}
error() { MSG=$1; msg "[ERROR] " "$MSG"; exit -1;}

usage() {
    echo "Usage: $0 [SERVICE_NAME]"
    exit -1
}

graphql_rest() {
    file=$1
    value=`cat ${file} | sed 's/\"/\\\"/g'` 
    query=$(echo $value|tr -d '\n')
    
    echo $query
    curl -i -XPOST $REST/graphql -H 'content-type: application/json' -d "
    {
        \"query\": \"$query\",
        \"variables\": null
    }"
    sleep 5
}
get_services() {
    curl -i -XPOST $REST/graphql -H 'content-type: application/json' -d '
    {
        "query": "query{Management{Services{id name }}}"
    }'
}
get_labels() {
    curl -i -XPOST $REST/graphql -H 'content-type: application/json' -d '
    {
        "query": "query{Management{Labels {id name}}}"
    }'
}
