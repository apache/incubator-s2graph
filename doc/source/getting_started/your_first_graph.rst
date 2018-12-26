Your First S2Graph
==================

Once the S2Graph server has been set up, you can now start to send HTTP queries to the server to create a graph and pour data in it. This tutorial goes over a simple toy problem to get a sense of how S2Graph's API looks like. For the exact definitions of the terminology used here, refer to The Data Model document.
The toy problem is to create a timeline feature for a simple social media, like a simplified version of Facebook's timeline. Using simple S2Graph queries it is possible to keep track of each user's friends and their posts.

First, we need a name for the new service.
---------------------------------------------

The following POST query will create a service named ``KakaoFavorites``

.. code:: bash

  curl -XPOST localhost:9000/admin/createService -H 'Content-Type: Application/json' -d '
  {
    "serviceName": "KakaoFavorites",
    "compressionAlgorithm" : "gz"
  }'

To make sure the service is created correctly, check out the following

.. code:: bash

  curl -XGET localhost:9000/admin/getService/KakaoFavorites

Next, we will need some friends.
---------------------------------------------
In S2Graph, relationships are organized as labels. Create a ``friends`` label with the following ``createLabel`` API call:

.. code:: bash

  curl -XPOST localhost:9000/admin/createLabel -H 'Content-Type: Application/json' -d '
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
  }'

Check if the label has been created correctly:

.. code:: bash

   curl -XGET localhost:9000/admin/getLabel/friends

Now that the label ``friends`` is ready, we can store the friendship data. Entries of a label are called edges, and you can add edges with ``edges/insert`` API:

.. code:: bash

   curl -XPOST localhost:9000/mutate/edge/insert -H 'Content-Type: Application/json' -d '
   [
      {"from":"Elmo","to":"Big Bird","label":"friends","props":{},"timestamp":1444360152477},
      {"from":"Elmo","to":"Ernie","label":"friends","props":{},"timestamp":1444360152478},
      {"from":"Elmo","to":"Bert","label":"friends","props":{},"timestamp":1444360152479},
      {"from":"Cookie Monster","to":"Grover","label":"friends","props":{},"timestamp":1444360152480},
      {"from":"Cookie Monster","to":"Kermit","label":"friends","props":{},"timestamp":1444360152481},
      {"from":"Cookie Monster","to":"Oscar","label":"friends","props":{},"timestamp":1444360152482}
   ]'

Query friends of Elmo with ``getEdges`` API:

.. code:: bash

  curl -XPOST localhost:9000/graphs/getEdges -H 'Content-Type: Application/json' -d '
  {
    "srcVertices": [{"serviceName": "KakaoFavorites", "columnName": "userName", "id":"Elmo"}],
    "steps": [
      {"step": [{"label": "friends", "direction": "out", "offset": 0, "limit": 10}]}
    ]
  }'

Now query friends of Cookie Monster:

.. code:: bash

  curl -XPOST localhost:9000/graphs/getEdges -H 'Content-Type: Application/json' -d '
  {
    "srcVertices": [{"serviceName": "KakaoFavorites", "columnName": "userName", "id":"Cookie Monster"}],
    "steps": [
      {"step": [{"label": "friends", "direction": "out", "offset": 0, "limit": 10}]}
    ]
  }'

Users of Kakao Favorites will be able to ``post`` URLs of their favorite websites.
----------------------------------------------------------------------------------

We will need a new label ``post`` for this data:

.. code:: bash

  curl -XPOST localhost:9000/admin/createLabel -H 'Content-Type: Application/json' -d '
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
  }'

Now, insert some posts of the users:

.. code:: bash

  curl -XPOST localhost:9000/mutate/edge/insert -H 'Content-Type: Application/json' -d '
  [
    {"from":"Big Bird","to":"www.kakaocorp.com/en/main","label":"post","props":{},"timestamp":1444360152477},
    {"from":"Big Bird","to":"github.com/kakao/s2graph","label":"post","props":{},"timestamp":1444360152478},
    {"from":"Ernie","to":"groups.google.com/forum/#!forum/s2graph","label":"post","props":{},"timestamp":1444360152479},
    {"from":"Grover","to":"hbase.apache.org/forum/#!forum/s2graph","label":"post","props":{},"timestamp":1444360152480},
    {"from":"Kermit","to":"www.playframework.com","label":"post","props":{},"timestamp":1444360152481},
    {"from":"Oscar","to":"www.scala-lang.org","label":"post","props":{},"timestamp":1444360152482}
  ]'


Query posts of Big Bird:

.. code:: bash

  curl -XPOST localhost:9000/graphs/getEdges -H 'Content-Type: Application/json' -d '
  {
    "srcVertices": [{"serviceName": "KakaoFavorites", "columnName": "userName", "id":"Big Bird"}],
    "steps": [
      {"step": [{"label": "post", "direction": "out", "offset": 0, "limit": 10}]}
    ]
  }'


So far, we have designed a label schema for the labels ``friends`` and ``post``, and stored some edges to them.
---------------------------------------------------------------------------------------------------------------

This should be enough for creating the timeline feature! The following two-step query will return the URLs for Elmo's timeline, which are the posts of Elmo's friends:


.. code:: bash

  curl -XPOST localhost:9000/graphs/getEdges -H 'Content-Type: Application/json' -d '
  {
    "srcVertices": [{"serviceName": "KakaoFavorites", "columnName": "userName", "id":"Elmo"}],
    "steps": [
      {"step": [{"label": "friends", "direction": "out", "offset": 0, "limit": 10}]},
      {"step": [{"label": "post", "direction": "out", "offset": 0, "limit": 10}]}
    ]
  }'

Also try Cookie Monster's timeline:

.. code:: bash

  curl -XPOST localhost:9000/graphs/getEdges -H 'Content-Type: Application/json' -d '
  {
    "srcVertices": [{"serviceName": "KakaoFavorites", "columnName": "userName", "id":"Cookie Monster"}],
    "steps": [
      {"step": [{"label": "friends", "direction": "out", "offset": 0, "limit": 10}]},
      {"step": [{"label": "post", "direction": "out", "offset": 0, "limit": 10}]}
    ]
  }'


The example above is by no means a full blown social network timeline, but it gives you an idea of how to represent, store and query graph data with S2Graph.

We also provide a simple script under ``script/test.sh`` so that you can see if everything is setup correctly.

.. code:: bash

  sh script/test.sh
