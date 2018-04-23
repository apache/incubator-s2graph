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
source common.sh

[ $# -ne 1 ] && { usage; }

SERVICE=$1
SERVICE_HOME=${WORKDIR}/${SERVICE}
SCHEMA_HOME=${SERVICE_HOME}/schema

info "schema dir : $SCHEMA_HOME"

q "generate input >>> " 
cd ${SERVICE_HOME}
sh ./generate_input.sh

q "create service >>> "
graphql_rest ${SCHEMA_HOME}/service.graphql
get_services

q "create vertices >>>"
for file in `ls ${SCHEMA_HOME}/vertex.*`; do
    info ""
    info "file:  $file"
    graphql_rest $file
done
get_services

q "create edges >>> "
for file in `ls ${SCHEMA_HOME}/edge.*`; do
    info ""
    info "file: $file"
    graphql_rest $file
done
get_labels
