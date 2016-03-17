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
# create service.
curl -XPOST localhost:9000/graphs/createService -H 'Content-Type: Application/json' -d '
{"serviceName": "s2graph", "compressionAlgorithm" : "gz"}
'
# check service.
curl -XGET localhost:9000/graphs/getService/s2graph

# create label.
curl -XPUT localhost:9000/graphs/deleteLabel/graph_test
curl -XPOST localhost:9000/graphs/createLabel -H 'Content-Type: Application/json' -d '
{
    "label": "graph_test",
    "srcServiceName": "s2graph",
    "srcColumnName": "account_id",
    "srcColumnType": "long",
    "tgtServiceName": "s2graph",
    "tgtColumnName": "item_id",
    "tgtColumnType": "string",
    "indices": [ { "name": "idx_time_weight", "propNames": ["time", "weight"]} ],
    "props": [
	{"name": "time", "dataType": "integer", "defaultValue": 0},
	{"name": "weight","dataType": "float","defaultValue": 0.0},
	{"name": "is_hidden","dataType": "boolean","defaultValue": false},
	{"name": "is_blocked","dataType": "boolean","defaultValue": false}
    ],
    "consistencyLevel": "strong"
}
'
curl -XPUT localhost:9000/graphs/deleteLabel/graph_test_2
curl -XPOST localhost:9000/graphs/createLabel -H 'Content-Type: Application/json' -d '
{
    "label": "graph_test_2",
    "srcServiceName": "s2graph",
    "srcColumnName": "account_id",
    "srcColumnType": "long",
    "tgtServiceName": "s2graph",
    "tgtColumnName": "item_id",
    "tgtColumnType": "string",
    "indices": [ { "name": "idx_time_weight", "propNames": ["time", "weight"]} ],
    "props": [
	{"name": "time", "dataType": "integer", "defaultValue": 0},
	{"name": "weight","dataType": "float","defaultValue": 0.0},
	{"name": "is_hidden","dataType": "boolean","defaultValue": false},
	{"name": "is_blocked","dataType": "boolean","defaultValue": false}
    ],
    "consistencyLevel": "strong"
}
'

# check labels
curl -XGET localhost:9000/graphs/getLabel/graph_test
curl -XGET localhost:9000/graphs/getLabel/graph_test_2

# app props
curl -XPOST localhost:9000/graphs/addProp/graph_test -H 'Content-Type: Application/json' -d '
{"name": "rel_type", "defaultValue": 0, "dataType": "integer"}
'

curl -XPOST localhost:9000/graphs/addProp/graph_test -H 'Content-Type: Application/json' -d '
{"name": "play_count", "defaultValue": 0, "dataType": "integer"}
'

curl -XPOST localhost:9000/graphs/addProp/graph_test -H 'Content-Type: Application/json' -d '
{"name": "pay_amount", "defaultValue": 0, "dataType": "integer"}
'

# check if props is added correctly
curl -XGET localhost:9000/graphs/getLabel/graph_test

# add extra index
curl -XPOST localhost:9000/graphs/addIndex -H 'Content-Type: Application/json' -d '
{
    "label": "graph_test",
    "indices": [ {"name": "idx_play_count_pay_amount", "propNames": ["play_count", "pay_amount"]} ]
}
'

# check if new index is added correcly
curl -XGET localhost:9000/graphs/getLabel/graph_test


# add edges
curl -XPOST localhost:9000/graphs/edges/insert -H 'Content-Type: Application/json' -d '
[
  {"from":1,"to":"ab","label":"graph_test","props":{"time":-1, "weight":0.98},"timestamp":1442502000000},
  {"from":1,"to":"123456","label":"graph_test","props":{"time":0, "weight":0.81},"timestamp":1442502000010},
  {"from":1,"to":"zdfdk2384","label":"graph_test","props":{"time":1, "weight":1.0},"timestamp":1442502000020},
  {"from":1,"to":"dfjkdjfdk1234","label":"graph_test","props":{"time":-2, "weight":0.71},"timestamp":1442502000030},
  {"from":1,"to":"ab","label":"graph_test_2","props":{"time":-1, "weight":0.98},"timestamp":1442502000040},
  {"from":1,"to":"123456","label":"graph_test_2","props":{"time":0, "weight":0.81},"timestamp":1442502000050},
  {"from":1,"to":"zdfdk2384","label":"graph_test_2","props":{"time":1, "weight":1.0},"timestamp":1442502000060},
  {"from":1,"to":"dfjkdjfdk1234","label":"graph_test_2","props":{"time":-2, "weight":0.71},"timestamp":1442502000070}
]
'

sleep 2

# select edges
curl -XPOST localhost:9000/graphs/getEdges -H 'Content-Type: Application/json' -d '
{
    "srcVertices": [{"serviceName": "s2graph", "columnName": "account_id", "id":1}],
    "steps": [
      {"step": [{"label": "graph_test", "direction": "out", "offset": 0, "limit": 10, "scoring": {"time": 1, "weight": 1}}]}
    ]
}
'
## check for contentions

curl -XPOST localhost:9000/graphs/edges/bulk -H 'Content-Type: text/plain' -d '
1442502000000	insert	edge	2	ab	graph_test	{"time": -1, "weight": 0.98}
1442502000001	delete	edge	2	ab	graph_test	{}
1442502000002	insert	edge	2	ab	graph_test	{"time": -10, "weight": -0.1}
'

curl -XPOST localhost:9000/graphs/getEdges -H 'Content-Type: Application/json' -d '
{
    "srcVertices": [{"serviceName": "s2graph", "columnName": "account_id", "id":2}],
    "steps": [
      {"step": [{"label": "graph_test", "direction": "out", "offset": 0, "limit": 10, "scoring": {"time": 1, "weight": 1}}]}
    ]
}
'

## Vertex
curl -XPOST localhost:9000/graphs/createServiceColumn -H 'Content-Type: Application/json' -d '
{
    "serviceName": "s2graph",
    "columnName": "user_id",
    "columnType": "long",
    "props": [
        {"name": "is_active", "dataType": "boolean", "defaultValue": true}, 
        {"name": "phone_number", "dataType": "string", "defaultValue": "-"},
        {"name": "nickname", "dataType": "string", "defaultValue": ".."},
        {"name": "activity_score", "dataType": "float", "defaultValue": 0.0},
        {"name": "age", "dataType": "integer", "defaultValue": 0}
    ]
}
' 
# add props on Vertex
curl -XPOST localhost:9000/graphs/addServiceColumnProps/s2graph/user_id -H 'Content-Type: Application/json' -d '
[
	{"name": "home_address", "defaultValue": "korea", "dataType": "string"}
]
'

# insert vertex data
curl -XPOST localhost:9000/graphs/vertices/insert/s2graph/user_id -H 'Content-Type: Application/json' -d '
[
  {"id":1,"props":{"is_active":true}, "timestamp":1417616431000},
  {"id":2,"props":{},"timestamp":1417616431000}
]
'


# select vertices
curl -XPOST localhost:9000/graphs/getVertices -H 'Content-Type: Application/json' -d '
[
    {"serviceName": "s2graph", "columnName": "user_id", "ids": [1, 2]}
]
'
