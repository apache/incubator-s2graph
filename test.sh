curl -XPOST localhost:9000/graphs/edges/insert -H 'Content-Type: Application/json' -d '
[
{"timestamp": 1447493110829, "from": 7007, "to": "700710007abc", "label": "s2graph_label_test_2", "props": {"time": 10}}
]
'
sleep 2
curl -XPOST localhost:9000/graphs/edges/insert -H 'Content-Type: Application/json' -d '
[
{"timestamp": 1447493110831, "from": 7007, "to": "700710007abc", "label": "s2graph_label_test_2", "props": {"time": -10, "weight": 20}}
]
'
sleep 2
curl -XPOST localhost:9000/graphs/edges/delete -H 'Content-Type: Application/json' -d '
[
{"timestamp": 1447493110830, "from": 7007, "to": "700710007abc", "label": "s2graph_label_test_2"}
]
'
sleep 2
curl -XPOST localhost:9000/graphs/getEdges -H 'Content-Type: Application/json' -d '
{
    "srcVertices": [
        {
            "serviceName": "s2graph",
            "columnName": "user_id_test",
            "id": 7007
        }
    ],
    "steps": [
        [
            {
                "label": "s2graph_label_test_2",
                "direction": "out",
                "offset": 0,
                "limit": -1,
                "duplicate": "raw"
            }
        ]
    ]
}
'

