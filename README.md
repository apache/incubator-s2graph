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

S2Graph [![Build Status](https://travis-ci.org/ijsong/incubator-s2graph.svg?branch=master)](https://travis-ci.org/ijsong/incubator-s2graph)
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

Once extracted the downloaded binary release of S2Graph or built from the source as described above, the following files and directories should be found in the directory.

```
DISCLAIMER
LICENCE               # the Apache License 2.0
NOTICE
bin                   # scripts to manage the lifecycle of S2Graph
conf                  # configuration files
lib                   # contains the binary
logs                  # application logs
var                   # application data
```

This directory layout contains all binary and scripts required to launch S2Graph. The directories `logs` and `var` may not be present initially, and are created once S2Graph is launched.

The following will launch S2Graph, using [HBase](https://hbase.apache.org/) in the standalone mode for data storage and [H2](http://www.h2database.com/html/main.html) as the metadata storage.

    sh bin/start-s2graph.sh

To connect to a remote HBase cluster or use MySQL as the metastore, refer to the instructions in [`conf/application.conf`](conf/application.conf).
S2Graph is tested on HBase versions 0.98, 1.0, 1.1, and 1.2 (https://hub.docker.com/r/harisekhon/hbase/tags/).

Project Layout
==============

Here is what you can find in each subproject.

1. `s2core`: The core library, containing the data abstractions for graph entities, storage adapters and utilities.
2. `s2rest_play`: The REST server built with [Play framework](https://www.playframework.com/), providing the write and query API.
3. `s2rest_netty`: The REST server built directly using Netty, implementing only the query API.
4. `loader`: A collection of Spark jobs for bulk loading streaming data into S2Graph.
5. `spark`: Spark utilities for `loader` and `s2counter_loader`.
6. `s2counter_core`: The core library providing data structures and logics for `s2counter_loader`.
7. `s2counter_loader`: Spark streaming jobs that consume Kafka WAL logs and calculate various top-*K* results on-the-fly.

The first three projects are for OLTP-style workloads, currently the main target of S2Graph. The other four projects could be helpful for OLAP-style or streaming workloads, especially for integrating S2Graph with [Apache Spark](https://spark.apache.org/) and/or [Kafka](https://kafka.apache.org/). Note that, the latter four projects are currently out-of-date, which we are planning to update and provide documentations in the upcoming releases.

Your First Graph
================

Once the S2Graph server has been set up, you can now start to send HTTP queries to the server to create a graph and pour some data in it. This tutorial goes over a simple toy problem to get a sense of how S2Graph's API looks like. [`bin/example.sh`](bin/example.sh) contains the example code below.

The toy problem is to create a timeline feature for a simple social media, like a simplified version of Facebook's timeline:stuck_out_tongue_winking_eye:. Using simple S2Graph queries it is possible to keep track of each user's friends and their posts.

1. First, we need a name for the new service.

  The following POST query will create a service named "KakaoFavorites".

  ```
  curl -XPOST localhost:9000/graphs/createService -H 'Content-Type: Application/json' -d '
  {"serviceName": "KakaoFavorites", "compressionAlgorithm" : "gz"}
  '
  ```

  To make sure the service is created correctly, check out the following.

  ```
  curl -XGET localhost:9000/graphs/getService/KakaoFavorites
  ```

2. Next, we will need some friends.

  In S2Graph, relationships are organized as labels. Create a label called `friends` using the following `createLabel` API call:

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

  Check if the label has been created correctly:+

  ```
  curl -XGET localhost:9000/graphs/getLabel/friends
  ```

  Now that the label `friends` is ready, we can store the friendship data. Entries of a label are called edges, and you can add edges with `edges/insert` API:

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

  Query friends of Elmo with `getEdges` API:

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

  Now, insert some posts of the users:


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

4. So far, we have designed a label schema for the labels `friends` and `post`, and stored some edges to them.+

  This should be enough for creating the timeline feature! The following two-step query will return the URLs for Elmo's timeline, which are the posts of Elmo's friends:

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

The example above is by no means a full blown social network timeline, but it gives you an idea of how to represent, store and query graph data with S2Graph.+


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
