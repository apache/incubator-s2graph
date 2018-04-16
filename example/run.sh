#!/usr/bin/env bash
source common.sh

SERVICE="movielens"
[ $# -gt 0 ] && { SERVICE=$1; }
DESC=`cat $SERVICE/desc.txt`

info ""
info ""
info "Let's try to create the toy project '$SERVICE' using s2graphql and s2jobs."
info ""
while IFS='' read -r line || [[ -n "$line" ]]; do
    info "$line"
done < "$SERVICE/desc.txt"

q "First of all, we will check prerequisites"
./prepare.sh $SERVICE
[ $? -ne 0 ] && { exit -1; }

q "And now, we create vertex and edge schema using graphql"
./create_schema.sh $SERVICE
[ $? -ne 0 ] && { exit -1; }

q "Finally, we import example data to service"
./import_data.sh $SERVICE
[ $? -ne 0 ] && { exit -1; }

