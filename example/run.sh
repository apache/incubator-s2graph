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

SERVICE="movielens"
[ $# -gt 0 ] && { SERVICE=$1; }
DESC=`cat $SERVICE/desc.md`

info ""
info ""
info "Let's try to create the toy project '$SERVICE' using s2graphql and s2jobs."
info ""
while IFS='' read -r line || [[ -n "$line" ]]; do
    info "$line"
done < "$SERVICE/desc.md"

q "First of all, we will check prerequisites"
sh ./prepare.sh $SERVICE
[ $? -ne 0 ] && { exit -1; }

q "And now, we create vertex and edge schema using graphql"
sh ./create_schema.sh $SERVICE
[ $? -ne 0 ] && { exit -1; }

q "Finally, we import example data to service"
sh ./import_data.sh $SERVICE
[ $? -ne 0 ] && { exit -1; }

