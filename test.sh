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

