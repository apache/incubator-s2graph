# create service.
curl -XPOST localhost:9000/graphs/createService -H 'Content-Type: Application/json' -d '
{"serviceName": "s2graph"}
'
# check service.
curl -XGET localhost:9000/graphs/getService/s2graph

# create label.
curl -XPOST localhost:9000/graphs/createLabel -H 'Content-Type: Application/json' -d '
{
    "label": "graph_test",
    "srcServiceName": "s2graph",
    "srcColumnName": "account_id",
    "srcColumnType": "long",
    "tgtServiceName": "s2graph",
    "tgtColumnName": "item_id",
    "tgtColumnType": "string",
    "indexProps": [
	{"name": "time", "dataType": "integer", "defaultValue": 0},
	{"name": "weight","dataType": "float","defaultValue": 0.0}
    ],
    "props": [
	{"name": "is_hidden","dataType": "boolean","defaultValue": false},
	{"name": "is_blocked","dataType": "boolean","defaultValue": false}
    ],
    "consistencyLevel": "strong"
}
'

# check label
curl -XGET localhost:9000/graphs/getLabel/graph_test

# app props
curl -XPOST localhost:9000/graphs/addProp/graph_test -H 'Content-Type: Application/json' -d '
{"name": "rel_type", "defaultValue": 0, "dataType": "integer"}
'

# check props is added correctly
curl -XGET localhost:9000/graphs/getLabel/graph_test

# add extra index
curl -XPOST localhost:9000/graphs/addIndex -H 'Content-Type: Application/json' -d '
{
    "label": "graph_test",
    "indexProps": [
        {"name": "play_count","defaultValue": 0, "dataType": "integer"},
        {"name": "pay_amount","defaultValue": 0,"dataType": "integer"}
    ]
}
'

# check new index is added correcly
curl -XGET localhost:9000/graphs/getLabel/graph_test


# add edges
curl -XPOST localhost:9000/graphs/edges/insert -H 'Content-Type: Application/json' -d '
[
  {"from":1,"to":"ab","label":"graph_test","props":{"time":-1, "weight":0.98},"timestamp":193829192},
  {"from":1,"to":"123456","label":"graph_test","props":{"time":0, "weight":0.81},"timestamp":193829192},
  {"from":1,"to":"zdfdk2384","label":"graph_test","props":{"time":1, "weight":1.0},"timestamp":193829192},
  {"from":1,"to":"dfjkdjfdk1234","label":"graph_test","props":{"time":-2, "weight":0.71},"timestamp":193829192}
]
'

sleep 2

# select edges
curl -XPOST localhost:9000/graphs/getEdges -H 'Content-Type: Application/json' -d '
{
    "srcVertices": [{"serviceName": "s2graph", "columnName": "account_id", "id":1}],
    "steps": [
      [{"label": "graph_test", "direction": "out", "offset": 0, "limit": 10, "scoring": {"time": 1, "weight": 1}}]
    ]
}
'

## Vertex
curl -XPOST localhost:9000/graphs/createVertex -H 'Content-Type: Application/json' -d ' 
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
curl -XPOST localhost:9000/graphs/addVertexProps/s2graph/user_id -H 'Content-Type: Application/json' -d '
[
	{"name": "home_address", "defaultValue": "korea", "dataType": "string"}
]
'

# insert vertex data
curl -XPOST localhost:9000/graphs/vertices/insert/s2graph/user_id -H 'Content-Type: Application/json' -d '
[
  {"id":1,"props":{"is_active":true}, "timestamp":1417616431},
  {"id":2,"props":{},"timestamp":1417616431}
]
'


# select vertices
curl -XPOST localhost:9000/graphs/getVertices -H 'Content-Type: Application/json' -d '
[
    {"serviceName": "s2graph", "columnName": "user_id", "ids": [1, 2, 3]}
]
'
