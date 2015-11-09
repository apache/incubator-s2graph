curl -XPUT localhost:9000/graphs/deleteLabel/insert_test
curl -XPOST localhost:9000/graphs/createLabel -H 'Content-Type: Application/json' -d '
{
    "label": "insert_test",
    "srcServiceName": "s2graph-test",
    "srcColumnName": "item_id_test",
    "srcColumnType": "string",
    "tgtServiceName": "s2graph-test",
    "tgtColumnName": "item_id_test",
    "tgtColumnType": "string",
    "isDirected" : "true",
    "indices": [],
    "props": [
        {"name": "lastSeq", "defaultValue": 0, "dataType": "long"}
        , {"name": "success", "defaultValue": false, "dataType": "boolean"}
    ],
    "consistencyLevel": "strong"
}'
