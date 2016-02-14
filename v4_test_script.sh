curl -XPOST localhost:9000/graphs/createService -H 'Content-Type: Application/json' -d '
{"serviceName": "s2graph-test"}
'
curl -XPUT localhost:9000/graphs/deleteLabel/label_test_v4

curl -XPOST localhost:9000/graphs/createLabel -H 'Content-Type: Application/json' -d '
{
    "label": "label_test_v4",
    "srcServiceName": "s2graph-test",
    "srcColumnName": "user_id",
    "srcColumnType": "string",
    "tgtServiceName": "s2graph-test",
    "tgtColumnName": "item_id",
    "tgtColumnType": "string",
    "indices": [],
    "props": [],
    "consistencyLevel": "weak",
    "schemaVersion": "v4",
    "serviceName": "s2graph-test"
}
'
curl -XGET localhost:9000/graphs/getLabel/label_test_v4
curl -XPOST localhost:9000/graphs/edges/insert -H 'Content-Type: Application/json' -d '
[
  {"timestamp": 1, "from": "1", "to": "101", "label": "label_test_v4"},
  {"timestamp": 2, "from": "1", "to": "102", "label": "label_test_v4"},
  {"timestamp": 3, "from": "1", "to": "103", "label": "label_test_v4"},
  {"timestamp": 4, "from": "2", "to": "102", "label": "label_test_v4"},
  {"timestamp": 5, "from": "2", "to": "103", "label": "label_test_v4"},
  {"timestamp": 6, "from": "2", "to": "104", "label": "label_test_v4"}
]
'

curl -XPOST localhost:9000/graphs/edges/deleteAll -H 'Content-Type: Application/json' -d '
[
  {"timestamp": 10, "ids": ["1"], "label": "label_test_v4", "direction": "out"}
]
'
