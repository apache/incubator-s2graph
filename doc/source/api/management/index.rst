Management APIs
==================
Admin Apis for Management Service, Label, Index ..

****************
Create a Service
****************

``Service`` is the top level abstraction in S2Graph which could be considered as a database in MySQL.

.. code:: bash

  POST /admin/createService

Service Fields
---------------

In order to create a Service, the following fields should be specified in the request.

.. csv-table:: Option
   :header: "Field Name", "Definition", "Data Type", "Example", "Note"
   :widths: 15, 30, 30, 30, 30

   "serviceName",	"User defined namespace",	"String",	"talk_friendship", "Required"
   "cluster",	"Zookeeper quorum address",	"String",	"abc.com:2181,abd.com:2181", "Optional"
   "hTableName",	"HBase table name",	"String",	"test", "Optional"
   "hTableTTL",	"Time to live setting for data","Integer", "86000", "Optional"
   "preSplitSize",	"Factor for the table pre-split size", "Integer", "1", "Optional"

.. list
   - By default, S2Graph looks for "hbase.zookeeper.quorum" in your application.conf. If "hbase.zookeeper.quorum" is undefined, this value is set as "localhost".


Basic Service Operations
--------------------------

You can create a service using the following API:

.. code:: bash

  curl -XPOST localhost:9000/admin/createService -H 'Content-Type: Application/json' -d '
  {
     "serviceName": "s2graph",
     "cluster": "address for zookeeper",
     "hTableName": "hbase table name",
     "hTableTTL": 86000,
     "preSplitSize": 2
  }'


****************
Create a Label
****************

A ``Label`` represents a relation between two serviceColumns. Labels are to S2Graph what tables are to RDBMS since they contain the schema information, i.e. descriptive information of the data being managed or indices used for efficient retrieval.
In most scenarios, defining an edge schema (in other words, label) requires a little more care compared to a vertex schema (which is pretty straightforward).
First, think about the kind of queries you will be using, then, model user actions or relations into ``edges`` and design a label accordingly.

.. code:: bash

  POST /admin/createLabel

Label Fields
---------------

A Label creation request includes the following information.

.. csv-table:: Option
   :header: "Field Name", "Definition", "Data Type", "Example", "Note"
   :widths: 15, 30, 30, 30, 30

   "label",	"Name of the relation",	"String",	"talk_friendship", "Required"
   "srcServiceName", "Source column's service",	"String",	"kakaotalk", "Required"
   "srcColumnName", "Source column's name",	"String",	"user_id", "Required"
   "srcColumnType", "Source column's data type","Long/Integer/String",	"string", "Required"
   "tgtServiceName", "Target column's service",	"String",	"kakaotalk/kakaomusic", "Optional"
   "tgtColumnName", "Target column's name",	"String",	"item_id", "Required"
   "tgtColumnType", "Target column's data type", "Long/Integer/String",	"string", "Required"
   "isDirected", "Wether the label is directed or undirected",	"True/False",	"true/false", "Optional. default is true"
   "serviceName", "Which service the label belongs to",	"String",	"kakaotalk", "Optional. tgtServiceName is used by default"
   "hTableName", "A dedicated HBase table to your Label",	"String",	"s2graph-batch", "Optional. Service hTableName is used by default"
   "hTableTTL", "Data time to live setting",	"Integer", "86000", "Optional. Service hTableTTL is used by default"
   "consistencyLevel", "If set to 'strong', only one edge is alowed between a pair of source/ target vertices. Set to 'weak', and multiple-edge is supported",	"String", "strong/weak", "Optional. default is 'weak'"


Props & Indices
----------------

A couple of key elements of a Label are its Properties (props) and indices.
Supplementary information of a Vertex or Edge can be stored as props. A single property can be defined in a simple key-value JSON as follows:

.. code:: json

   {
     "name": "name of property",
     "dataType": "data type of property value",
     "defaultValue": "default value in string"
   }

In a scenario where user - video playback history is stored in a Label, a typical example for props would look like this:

.. code:: json

   [
     {"name": "play_count", "defaultValue": 0, "dataType": "integer"},
     {"name": "is_hidden","defaultValue": false,"dataType": "boolean"},
     {"name": "category","defaultValue": "jazz","dataType": "string"},
     {"name": "score","defaultValue": 0,"dataType": "float"}
   ]

Props can have data types of ``numeric`` (byte/ short/ integer/ float/ double), ``boolean`` or ``string``.
In order to achieve efficient data retrieval, a Label can be indexed using the "indices" option.
Default value for indices is ``_timestamp``, a hidden label property.

All labels have ``_timestamp`` in their props under the hood

The first index in indices array will be the primary index ``(Think of PRIMARY INDEX idx_xxx(p1, p2) in MySQL)``
S2Graph will automatically store edges according to the primary index.
Trailing indices are used for multiple ordering on edges. ``(Think of ALTER TABLE ADD INDEX idx_xxx(p2, p1) in MySQL)``

props define meta datas that will not be affect the order of edges.
Please avoid using S2Graph-reserved property names:


- ``_timestamp`` is reserved for system wise timestamp. this can be interpreted as last_modified_at
- ``_from`` is reserved for label's start vertex.
- ``_to`` is reserved for label's target vertex.


Basic Label Operations
--------------------------

Here is an sample request that creates a label ``user_article_liked`` between column ``user_id`` of service ``s2graph`` and column ``article_id`` of service ``s2graph_news``.
Note that the default indexed property ``_timestamp`` will be created since the ``indexedProps`` field is empty.

.. code:: bash

   curl -XPOST localhost:9000/admin/createLabel -H 'Content-Type: Application/json' -d '
   {
     "label": "user_article_liked",
     "srcServiceName": "s2graph",
     "srcColumnName": "user_id",
     "srcColumnType": "long",
     "tgtServiceName": "s2graph_news",
     "tgtColumnName": "article_id",
     "tgtColumnType": "string",
     "indices": [], // _timestamp will be used as default
     "props": [],
     "serviceName": "s2graph_news"
   }'


The created label ``user_article_liked`` will manage edges in a timestamp-descending order (which seems to be the common requirement for most services).
Here is another example that creates a label ``friends``, which represents the friend relation between ``users`` in service ``s2graph``.
This time, edges are managed by both affinity_score and ``_timestamp``.

Friends with higher affinity_scores come first and if affinity_score is a tie, recently added friends comes first.

.. code:: bash

   curl -XPOST localhost:9000/admin/createLabel -H 'Content-Type: Application/json' -d '
   {
     "label": "friends",
     "srcServiceName": "s2graph",
     "srcColumnName": "user_id",
     "srcColumnType": "long",
     "tgtServiceName": "s2graph",
     "tgtColumnName": "user_id",
     "tgtColumnType": "long",
     "indices": [
       {"name": "idx_affinity_timestamp", "propNames": ["affinity_score", "_timestamp"]}
     ],
     "props": [
       {"name": "affinity_score", "dataType": "float", "defaultValue": 0.0},
       {"name": "_timestamp", "dataType": "long", "defaultValue": 0},
       {"name": "is_hidden", "dataType": "boolean", "defaultValue": false},
       {"name": "is_blocked", "dataType": "boolean", "defaultValue": true},
       {"name": "error_code", "dataType": "integer", "defaultValue": 500}
     ],
     "serviceName": "s2graph",
     "consistencyLevel": "strong"
     }'

S2Graph supports **multiple indices** on a label which means you can add separate ordering options for edges.


.. code:: bash

    curl -XPOST localhost:9000/admin/addIndex -H 'Content-Type: Application/json' -d '
    {
      "label": "friends",
      "indices": [
        {"name": "idx_3rd", "propNames": ["is_blocked", "_timestamp"]}
      ]
    }'

In order to get general information on a label, make a GET request to ``/admin/getLabel/{label name}``

.. code:: bash

   curl -XGET localhost:9000/admin/getLabel/friends


Delete a label with a PUT request to ``/admin/deleteLabel/{label name}``

.. code:: bash

   curl -XPUT localhost:9000/admin/deleteLabel/friends


Label updates are not supported (except when you are adding an index). Instead, you can delete the label and re-create it.

Adding Extra Properties to Labels
----------------------------------

To add a new property, use ``/admin/addProp/{label name}``

.. code:: bash

   curl -XPOST localhost:9000/admin/addProp/friend -H 'Content-Type: Application/json' -d '
   {
     "name": "is_blocked",
     "defaultValue": false,
     "dataType": "boolean"
   }'


Consistency Level
------------------

Simply put, the consistency level of your label will determine how the edges are stored at storage level.
First, note that S2Graph identifies a unique edge by combining its from, label, to values as a key.

Now, let's consider inserting the following two edges that have same keys (1, graph_test, 101) and different timestamps (1418950524721 and 1418950524723).

.. code:: bash

   1418950524721    insert  e 1 101    graph_test    {"weight": 10} = (1, graph_test, 101)
   1418950524723    insert  e 1 101    graph_test    {"weight": 20} = (1, graph_test, 101)


**Each consistency levels handle the case differently.**

- strong

  - The strong option makes sure that there is only one edge record stored in the HBase table for edge key (1, graph_test, 101). With strong consistency level, the later insertion will overwrite the previous one.

- weak

  - The weak option will allow two different edges stored in the table with different timestamps and weight values.


For a better understanding, let's simplify the notation for an edge that connects two vertices u - v at time t as u -> (t, v), and assume that we are inserting these four edges into two different labels with each consistency configuration (both indexed by timestamp only).

.. code:: bash

   u1 -> (t1, v1)
   u1 -> (t2, v2)
   u1 -> (t3, v2)
   u1 -> (t4, v1)

With a strong consistencyLevel, your Label contents will be:

.. code:: bash

   u1 -> (t4, v1)
   u1 -> (t3, v2)

Note that edges with same vertices and earlier timestamp (u1 -> (t1, v1) and u1 -> (t2, v2)) were overwritten and do not exist.
On the other hand, with consistencyLevel weak.

.. code:: bash

   u1 -> (t1, v1)
   u1 -> (t2, v2)
   u1 -> (t3, v2)
   u1 -> (t4, v1)

**It is recommended to set consistencyLevel to weak unless you are expecting concurrent updates on same edge.**

In real world systems, it is not guaranteed that operation requests arrive at S2Graph in the order of their timestamp. Depending on the environment (network conditions, client making asynchronous calls, use of a message que, and so on) request that were made earlier can arrive later. Consistency level also determines how S2Graph handles these cases.
Strong consistencyLevel promises a final result consistent to the timestamp.
For example, consider a set of operation requests on edge (1, graph_test, 101) were made in the following order;


.. code:: bash

   1418950524721    insert    e    1    101    graph_test    {"is_blocked": false}
   1418950524722    delete    e    1    101    graph_test
   1418950524723    insert    e    1    101    graph_test    {"is_hidden": false, "weight": 10}
   1418950524724    update    e    1    101    graph_test    {"time": 1, "weight": -10}
   1418950524726    update    e    1    101    graph_test    {"is_blocked": true}


and actually arrived in a shuffled order due to complications


.. code:: bash

   1418950524726    update    e    1    101    graph_test    {"is_blocked": true}
   1418950524723    insert    e    1    101    graph_test    {"is_hidden": false, "weight": 10}
   1418950524722    delete    e    1    101    graph_test
   1418950524721    insert    e    1    101    graph_test    {"is_blocked": false}
   1418950524724    update    e    1    101    graph_test    {"time": 1, "weight": -10}

Strong consistency still makes sure that you get the same eventual state on (1, graph_test, 101).
Here is pseudocode of what S2Graph does to provide a strong consistency level.

.. code:: bash

   complexity = O(one read) + O(one delete) + O(2 put)

   fetchedEdge = fetch edge with (1, graph_test, 101) from lookup table.

   if fetchedEdge is not exist:
          create new edge same as current insert operation
          update lookup table as current insert operation
   else:
          valid = compare fetchedEdge vs current insert operation.
          if valid:
          delete fetchedEdge
          create new edge after comparing fetchedEdge and current insert.
          update lookup table

Limitations Since S2Graph makes asynchronous writes to HBase via Asynchbase, there is no consistency guaranteed on same edge within its flushInterval (1 second).

Adding Extra Indices (Optional)
---------------------------------

.. code:: bash

   POST /admin/addIndex

A label can have multiple properties set as indexes. When edges are queried, the ordering will determined according to indexes, therefore, deciding which edges will be included in the top-K results.

**Edge retrieval queries in S2Graph by default returns top-K edges. Clients must issue another query to fetch the next K edges, i.e., top-K ~ 2 x top-K**

Edges sorted according to the indices in order to limit the number of edges being fetched by a query. If no ordering property is given, S2Graph will use the timestamp as an index, thus resulting in the most recent data.

**It would be extremely difficult to fetch millions of edges and sort them at request time and return a top-K in a reasonable amount of time. Instead, S2Graph uses vertex-centric indexes to avoid this.
Using a vertex-centric index, having millions of edges is fine as long as size K of the top-K values is reasonable (under 1K) Note that indexes must be created prior to inserting any data on the label (which is the same case with the conventional RDBMS).**

New indexes can be dynamically added, but will not be applied to pre-existing data (support for this is planned for future versions). Currently, a label can have up to eight indices.
The following is an example of adding index ``play_count`` to a label ``graph_test``.

.. code:: bash

   # add prop first
   curl -XPOST localhost:9000/admin/addProp/graph_test -H 'Content-Type: Application/json' -d '
   { "name": "play_count", "defaultValue": 0, "dataType": "integer" }'

   # then add index
   curl -XPOST localhost:9000/admin/addIndex -H 'Content-Type: Application/json' -d '
   {
     "label": "graph_test",
      "indices": [
        { name: "idx_play_count", propNames: ["play-count"] }
      ]
   }'


**********************************
Create a ServiceColumn (Optional)
**********************************

.. code:: bash

   POST /admin/createServiceColumn

If your use case requires props assigned to vertices instead of edges, what you need is a Service Column

**Remark: If it is only the vertex id that you need and not additional props, there's no need to create a Service Column explicitly. At label creation, by default, S2Graph creates column space with empty properties according to the label schema.**


Service Column Fields
----------------------


.. csv-table:: Option
   :header: "Field Name", "Definition", "Data Type", "Example", "Note"
   :widths: 15, 30, 30, 30, 30

   "Field Name",	"Definition",	"Data Type",	"Example",	"Remarks"
   "serviceName", "Which service the Service Column belongs to", "String", "kakaotalk", "Required"
   "columnName", "Service Column`s name", "String", "talk_user_id", "Required"
   "props", "Optional properties of Service Column",	"JSON (array dictionaries)", "Please refer to the examples", "Optional"


Basic Service Column Operations
-------------------------------

Here are some sample requests for Service Column creation as well as vertex insertion and selection.


.. code:: bash

   curl -XPOST localhost:9000/admin/createServiceColumn -H 'Content-Type: Application/json' -d '
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
   }'

General information on a vertex schema can be retrieved with ``/admin/getServiceColumn/{service name}/{column name}``

.. code:: bash

   curl -XGET localhost:9000/admin/getServiceColumn/s2graph/user_id

This will give all properties on serviceName ``s2graph`` and columnName ``user_id`` serviceColumn.
Properties can be added to a Service Column with ``/admin/addServiceColumnProps/{service name}/{column name}``

.. code:: bash

   curl -XPOST localhost:9000/admin/addServiceColumnProps/s2graph/user_id -H 'Content-Type: Application/json' -d '
   [
     {"name": "home_address", "defaultValue": "korea", "dataType": "string"}
   ]'

Vertices can be inserted to a Service Column using ``/admin/vertices/insert/{service name}/{column name}``

.. code:: bash

   curl -XPOST localhost:9000/mutate/vertex/insert/s2graph/user_id -H 'Content-Type: Application/json' -d '
   [
     {"id":1,"props":{"is_active":true}, "timestamp":1417616431},
     {"id":2,"props":{},"timestamp":1417616431}
   ]'

Finally, query your vertex via ``/graphs/getVertices``

.. code:: bash

   curl -XPOST localhost:9000/graphs/getVertices -H 'Content-Type: Application/json' -d '
   [
     {"serviceName": "s2graph", "columnName": "user_id", "ids": [1, 2, 3]}
   ]'
