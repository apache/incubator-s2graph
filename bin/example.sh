#!/usr/bin/env bash


printf "First, we need a name for the new service. \n Why don't we call it Kakao Favorites? \n"
read -r -p 'Step 1: Creating Service >>> ' var

curl -XPOST localhost:9000/graphs/createService -H 'Content-Type: Application/json' -d '
{"serviceName": "KakaoFavorites", "compressionAlgorithm" : "gz"}
'

printf "\n\n"

read -r -p 'Make Sure the service is created correctly. >>> ' var

curl -XGET localhost:9000/graphs/getService/KakaoFavorites

# 2. Next, we will need some friends.
# In S2Graph, relationships are defined as Labels.
# Create a friends label with the following createLabel API call:
printf "\n\n\nNext, we will need some friends. \nIn S2Graph, relationships are defined as Labels.\nCreate a friends label with the following createLabel API call:\n"
read -r -p 'Step 2: Create Label >>> ' var

payload='
{
  "label": "friends",
  "srcServiceName": "KakaoFavorites",
  "srcColumnName": "userName",
  "srcColumnType": "string",
  "tgtServiceName": "KakaoFavorites",
  "tgtColumnName": "userName",
  "tgtColumnType": "string",
  "isDirected": "false",
  "indices": [],
  "props": [],
  "consistencyLevel": "strong"
}
'
printf "\n$payload\n"
curl -XPOST localhost:9000/graphs/createLabel -H 'Content-Type: Application/json' -d '
{
  "label": "friends",
  "srcServiceName": "KakaoFavorites",
  "srcColumnName": "userName",
  "srcColumnType": "string",
  "tgtServiceName": "KakaoFavorites",
  "tgtColumnName": "userName",
  "tgtColumnType": "string",
  "isDirected": "false",
  "indices": [],
  "props": [],
  "consistencyLevel": "strong"
}
'

# Check the label:
printf "\n\n"
read -r -p 'Make Sure Label has been created correctly >>> ' var

curl -XGET localhost:9000/graphs/getLabel/friends

# Now that the label friends is ready, we can store friend entries.
# Entries of a label are called edges, and you can add edges with the edges/insert API:
printf "\n\nNow that the label friends is ready, we can store friend entries.\nEntries of a label are called edges, and you can add edges with the edges/insert API:\n"
read -r -p 'Step 3: Insert Edges >>> ' var
payload='
[
  {"from":"Elmo","to":"Big Bird","label":"friends","props":{},"timestamp":1444360152477},
  {"from":"Elmo","to":"Ernie","label":"friends","props":{},"timestamp":1444360152478},
  {"from":"Elmo","to":"Bert","label":"friends","props":{},"timestamp":1444360152479},

  {"from":"Cookie Monster","to":"Grover","label":"friends","props":{},"timestamp":1444360152480},
  {"from":"Cookie Monster","to":"Kermit","label":"friends","props":{},"timestamp":1444360152481},
  {"from":"Cookie Monster","to":"Oscar","label":"friends","props":{},"timestamp":1444360152482}
]
'
printf "\n$payload\n"
curl -XPOST localhost:9000/graphs/edges/insert -H 'Content-Type: Application/json' -d '
[
  {"from":"Elmo","to":"Big Bird","label":"friends","props":{},"timestamp":1444360152477},
  {"from":"Elmo","to":"Ernie","label":"friends","props":{},"timestamp":1444360152478},
  {"from":"Elmo","to":"Bert","label":"friends","props":{},"timestamp":1444360152479},

  {"from":"Cookie Monster","to":"Grover","label":"friends","props":{},"timestamp":1444360152480},
  {"from":"Cookie Monster","to":"Kermit","label":"friends","props":{},"timestamp":1444360152481},
  {"from":"Cookie Monster","to":"Oscar","label":"friends","props":{},"timestamp":1444360152482}
]
'
printf "\n\n"
read -r -p 'Step 4: Query friends of Elmo with getEdges API: >>> ' var

# Query friends of Elmo with getEdges API:
curl -XPOST localhost:9000/graphs/getEdges -H 'Content-Type: Application/json' -d '
{
    "srcVertices": [{"serviceName": "KakaoFavorites", "columnName": "userName", "id":"Elmo"}],
    "steps": [
      {"step": [{"label": "friends", "direction": "out", "offset": 0, "limit": 10}]}
    ]
}
'
printf "\n\n"
read -r -p 'Step 5: Now query friends of Cookie Monster: >>> ' var
# Now query friends of Cookie Monster:
curl -XPOST localhost:9000/graphs/getEdges -H 'Content-Type: Application/json' -d '
{
    "srcVertices": [{"serviceName": "KakaoFavorites", "columnName": "userName", "id":"Cookie Monster"}],
    "steps": [
      {"step": [{"label": "friends", "direction": "out", "offset": 0, "limit": 10}]}
    ]
}
'


# 3. Users of Kakao Favorites will be able to post URLs of their favorite websites.
# We will need a new label post for this data:
printf "\n\nUsers of Kakao Favorites will be able to post URLs of their favorite websites.\nWe will need a new label post for this data\n"
read -r -p 'Step 6: Create Label for Users of Kakao Favorites will be able to post URLs of their favorite websites. >>> ' var
payload='
{
  "label": "post",
  "srcServiceName": "KakaoFavorites",
  "srcColumnName": "userName",
  "srcColumnType": "string",
  "tgtServiceName": "KakaoFavorites",
  "tgtColumnName": "url",
  "tgtColumnType": "string",
  "isDirected": "true",
  "indices": [],
  "props": [],
  "consistencyLevel": "strong"
}
'
printf "\n$payload\n"
curl -XPOST localhost:9000/graphs/createLabel -H 'Content-Type: Application/json' -d '
{
  "label": "post",
  "srcServiceName": "KakaoFavorites",
  "srcColumnName": "userName",
  "srcColumnType": "string",
  "tgtServiceName": "KakaoFavorites",
  "tgtColumnName": "url",
  "tgtColumnType": "string",
  "isDirected": "true",
  "indices": [],
  "props": [],
  "consistencyLevel": "strong"
}
'

# Now, insert some posts of our users:

payload='
[
  {"from":"Big Bird","to":"www.kakaocorp.com/en/main","label":"post","props":{},"timestamp":1444360152477},
  {"from":"Big Bird","to":"github.com/kakao/s2graph","label":"post","props":{},"timestamp":1444360152478},
  {"from":"Ernie","to":"groups.google.com/forum/#!forum/s2graph","label":"post","props":{},"timestamp":1444360152479},
  {"from":"Grover","to":"hbase.apache.org/forum/#!forum/s2graph","label":"post","props":{},"timestamp":1444360152480},
  {"from":"Kermit","to":"www.playframework.com","label":"post","props":{},"timestamp":1444360152481},
  {"from":"Oscar","to":"www.scala-lang.org","label":"post","props":{},"timestamp":1444360152482}
]
'
printf "\n\n"
read -r -p 'Step 7: Now, insert some posts of our users. >>> ' var
printf "\n$payload\n"
curl -XPOST localhost:9000/graphs/edges/insert -H 'Content-Type: Application/json' -d '
[
  {"from":"Big Bird","to":"www.kakaocorp.com/en/main","label":"post","props":{},"timestamp":1444360152477},
  {"from":"Big Bird","to":"github.com/kakao/s2graph","label":"post","props":{},"timestamp":1444360152478},
  {"from":"Ernie","to":"groups.google.com/forum/#!forum/s2graph","label":"post","props":{},"timestamp":1444360152479},
  {"from":"Grover","to":"hbase.apache.org/forum/#!forum/s2graph","label":"post","props":{},"timestamp":1444360152480},
  {"from":"Kermit","to":"www.playframework.com","label":"post","props":{},"timestamp":1444360152481},
  {"from":"Oscar","to":"www.scala-lang.org","label":"post","props":{},"timestamp":1444360152482}
]
'

# Query posts of Big Bird:
printf "\n\n"
read -r -p 'Step 8: Query posts of Big Bird. >>> ' var
curl -XPOST localhost:9000/graphs/getEdges -H 'Content-Type: Application/json' -d '
{
    "srcVertices": [{"serviceName": "KakaoFavorites", "columnName": "userName", "id":"Big Bird"}],
    "steps": [
      {"step": [{"label": "post", "direction": "out", "offset": 0, "limit": 10}]}
    ]
}
'


# 4. So far, we designed a label schema for your user relation data friends and post as well as stored some sample edges.
# While doing so, we have also prepared ourselves for our timeline query!
# The following two-step query will return URLs for Elmo's timeline, which are posts of Elmo's friends:
printf "\n\n"
read -r -p 'Step 9: Elmo`s Timeline. >>> ' var
curl -XPOST localhost:9000/graphs/getEdges -H 'Content-Type: Application/json' -d '
{
    "srcVertices": [{"serviceName": "KakaoFavorites", "columnName": "userName", "id":"Elmo"}],
    "steps": [
      {"step": [{"label": "friends", "direction": "out", "offset": 0, "limit": 10}]},
      {"step": [{"label": "post", "direction": "out", "offset": 0, "limit": 10}]}
    ]
}
'

# Also try Cookie Monster's timeline:
printf "\n\n"
read -r -p 'Step 10: Also try Cookie Monsters timeline: >>> ' var
curl -XPOST localhost:9000/graphs/getEdges -H 'Content-Type: Application/json' -d '
{
    "srcVertices": [{"serviceName": "KakaoFavorites", "columnName": "userName", "id":"Cookie Monster"}],
    "steps": [
      {"step": [{"label": "friends", "direction": "out", "offset": 0, "limit": 10}]},
      {"step": [{"label": "post", "direction": "out", "offset": 0, "limit": 10}]}
    ]
}
'

printf "\n\n"