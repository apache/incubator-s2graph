
**S2Graph**
===================

**S2Graph** is a **graph database** designed to handle transactional graph processing at scale. Its REST API allows you to store, manage and query relational information using **edge** and **vertex** representations in a **fully asynchronous** and **non-blocking** manner. This document covers some basic concepts and terms of S2Graph as well as help you get a feel for the S2Graph API.

**Quick Start (with Vagrant)**
==

S2Graph comes with a Vagrantfile that lets you spin up a virtual environment for test and development purposes.
(On setting up S2Graph in your local environment directly, please refer to [Quick Start in Your Local Environment](https://steamshon.gitbooks.io/s2graph-book/content/getting_started.html).)

You will need [VirtualBox](https://www.virtualbox.org/wiki/Downloads) and [Vagrant](http://docs.ansible.com/ansible/intro_installation.html) installed on your system.

With everything ready, let's get started by running the following commands:

```
git clone https://github.com/kakao/s2graph.git
cd s2graph
vagrant up
vagrant ssh

// in the virtual environment..
cd s2graph
activator run
```

Finally, join the [mailing list](https://groups.google.com/forum/#!forum/s2graph)!


**Your First Graph**
==

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

  The following two-step query will return ULRs for Elmo's timeline, which are posts of Elmo's friends:

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

If you'd like to know more about S2Graph and its powerful APIs, please continue to the [S2Graph API document](https://www.gitbook.com/book/steamshon/s2graph-book)!


#### [S2Graph API document](https://steamshon.gitbooks.io/s2graph-book/content/)

#### [Mailing List](https://groups.google.com/forum/#!forum/s2graph)

[![Analytics](https://ga-beacon.appspot.com/UA-62888350-1/s2graph/readme.md)](https://github.com/kakao/s2graph)
