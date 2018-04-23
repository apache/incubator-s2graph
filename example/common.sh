#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
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
    value=`cat ${file} | sed 's/\"/\\\"/g' | grep "^[^#]"`
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
