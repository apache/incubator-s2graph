#!/usr/bin/env bash
source common.sh

[ $# -ne 1 ] && { usage; }

SERVICE=$1
SERVICE_HOME=${WORKDIR}/${SERVICE}
SCHEMA_HOME=${SERVICE_HOME}/schema

info "schema dir : $SCHEMA_HOME"

q "generate input >>> " 
cd ${SERVICE_HOME}
./generate_input.sh

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
