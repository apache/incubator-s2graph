<!---
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
--->

S2Graph
=======

[**S2Graph**](http://s2graph.apache.org/) is a **graph database** designed to handle transactional graph processing at scale. Its REST API allows you to store, manage and query relational information using **edge** and **vertex** representations in a **fully asynchronous** and **non-blocking** manner. This document covers some basic concepts and terms of S2Graph as well as help you get a feel for the S2Graph API.

Building from the source
========================

To build S2Graph from the source, install the JDK 8 and [SBT](http://www.scala-sbt.org/), and run the following command in the project root:

    sbt package

This will create a distribution of S2Graph that is ready to be deployed.

One can find distribution on `target/apache-s2graph-$version-incubating-bin`.

Quick Start
===========
Once built from source, the following resources are expected under `target/apache-s2graph-$version-incubating-bin`. This should be the same structure as in the binary distribution.

1. DISCLAIMER	
2. LICENSE
3. NOTICE
4. bin
5. conf
6. lib	
7. logs
8. var

`sh bin/start-s2graph.sh` will launch the S2Graph server along with a standalone [HBase server](https://hbase.apache.org/) as the data storage and [H2](http://www.h2database.com/html/main.html) as the metastore

To see how to connect remote HBase and S2Graph configurations, check out conf/application.conf.
Currently we have tested with these HBase version(0.98, 1.0, 1.1, 1.2)(https://hub.docker.com/r/harisekhon/hbase/tags/).

Finally, join the mailing list by sending a message to [users-subscribe@s2graph.incubator.apache.org](mailto:users-subscribe@s2graph.incubator.apache.org?subject=send this email to subscribe) or [dev-subscribe@s2graph.incubator.apache.org](mailto:dev-subscribe@s2graph.incubator.apache.org?subject=send this email to subscribe)!

Your First Graph
================

As a toy problem, let's try to create the backend for a simple timeline of a new social network service. (Think of a simplified version of Facebook's Timeline. :stuck_out_tongue_winking_eye:)
You will be able to manage "friends" and "posts" of a user with simple S2Graph queries.


1. First, we need a name for the new service.

  Why don't we call it Kakao Favorites?
  ```
  curl -XPOST localhost:9000/graphs/createService -H 'Content-Type: Application/json' -d '
  {"serviceName": "KakaoFavorites", "compressionAlgorithm" : "gz"}
  '
  ```

  Make sure the service is created correctly.
  ```
  curl -XGET localhost:9000/graphs/getService/KakaoFavorites
  ```

2. Next, we will need some friends.

  In S2Graph, relationships are defined as Labels.

  Create a ```friends``` label with the following ```createLabel``` API call:
  ```
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
  ```
  Check the label:
  ```
  curl -XGET localhost:9000/graphs/getLabel/friends
  ```

  Now that the label ```friends``` is ready, we can store friend entries.

  Entries of a label are called edges, and you can add edges with the ```edges/insert``` API:
  ```
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
  ```

  Query friends of Elmo with ```getEdges``` API:
  ```
  curl -XPOST localhost:9000/graphs/getEdges -H 'Content-Type: Application/json' -d '
  {
      "srcVertices": [{"serviceName": "KakaoFavorites", "columnName": "userName", "id":"Elmo"}],
      "steps": [
        {"step": [{"label": "friends", "direction": "out", "offset": 0, "limit": 10}]}
      ]
  }
  '
  ```

  Now query friends of Cookie Monster:
  ```
  curl -XPOST localhost:9000/graphs/getEdges -H 'Content-Type: Application/json' -d '
  {
      "srcVertices": [{"serviceName": "KakaoFavorites", "columnName": "userName", "id":"Cookie Monster"}],
      "steps": [
        {"step": [{"label": "friends", "direction": "out", "offset": 0, "limit": 10}]}
      ]
  }
  '
  ```

3. Users of Kakao Favorites will be able to post URLs of their favorite websites.

  We will need a new label ```post``` for this data:
  ```
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
  ```

  Now, insert some posts of our users:
  ```
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
  ```

  Query posts of Big Bird:
  ```
  curl -XPOST localhost:9000/graphs/getEdges -H 'Content-Type: Application/json' -d '
  {
      "srcVertices": [{"serviceName": "KakaoFavorites", "columnName": "userName", "id":"Big Bird"}],
      "steps": [
        {"step": [{"label": "post", "direction": "out", "offset": 0, "limit": 10}]}
      ]
  }
  '
  ```

4. So far, we designed a label schema for your user relation data ```friends``` and ```post``` as well as stored some sample edges.

  While doing so, we have also prepared ourselves for our timeline query!

  The following two-step query will return URLs for Elmo's timeline, which are posts of Elmo's friends:

  ```
  curl -XPOST localhost:9000/graphs/getEdges -H 'Content-Type: Application/json' -d '
  {
      "srcVertices": [{"serviceName": "KakaoFavorites", "columnName": "userName", "id":"Elmo"}],
      "steps": [
        {"step": [{"label": "friends", "direction": "out", "offset": 0, "limit": 10}]},
        {"step": [{"label": "post", "direction": "out", "offset": 0, "limit": 10}]}
      ]
  }
  '
  ```

  Also try Cookie Monster's timeline:
  ```
  curl -XPOST localhost:9000/graphs/getEdges -H 'Content-Type: Application/json' -d '
  {
      "srcVertices": [{"serviceName": "KakaoFavorites", "columnName": "userName", "id":"Cookie Monster"}],
      "steps": [
        {"step": [{"label": "friends", "direction": "out", "offset": 0, "limit": 10}]},
        {"step": [{"label": "post", "direction": "out", "offset": 0, "limit": 10}]}
      ]
  }
  '
  ```

The above example is by no means a full-blown social network timeline, but it gives you an idea on how to represent, store and query relations with S2Graph.

#### [The Official Website](https://s2graph.apache.org/)

#### [S2Graph API document](https://steamshon.gitbooks.io/s2graph-book/content/)

#### Mailing Lists

* [users@s2graph.incubator.apache.org](mailto:users@s2graph.incubator.apache.org) is for usage questions and announcements.
[(subscribe)](mailto:users-subscribe@s2graph.incubator.apache.org?subject=send this email to subscribe)
[(unsubscribe)](mailto:users-unsubscribe@s2graph.incubator.apache.org?subject=send this email to unsubscribe)
[(archives)](http://markmail.org/search/?q=list%3Aorg.apache.s2graph.users)
* [dev@s2graph.incubator.apache.org](mailto:dev@s2graph.incubator.apache.org) is for people who want to contribute to S2Graph.
[(subscribe)](mailto:dev-subscribe@s2graph.incubator.apache.org?subject=send this email to subscribe)
[(unsubscribe)](mailto:dev-unsubscribe@s2graph.incubator.apache.org?subject=send this email to unsubscribe)
[(archives)](http://markmail.org/search/?q=list%3Aorg.apache.s2graph.dev)

[![Analytics](https://ga-beacon.appspot.com/UA-62888350-1/s2graph/readme.md)](https://github.com/kakao/s2graph)
