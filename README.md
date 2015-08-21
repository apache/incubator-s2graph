

**s2graph**
===================

**s2graph** is a **GraphDB** that stores big data using **edges** and **vertices**, and also serves REST APIs for querying information on its edges and vertices. It provide fully  **asynchronous, non-blocking API to manupulate and traverse(breadth first search) large graph**. This document defines terms and concepts used in s2graph and describes its REST API. 


Table of content
-------------
<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](http://doctoc.herokuapp.com/)*

- [Getting Started](#getting-started)
- [The Data Model](#the-data-model)
- [REST API Glossary](#rest-api-glossary)
- [0. Create a Service - `POST /graphs/createService`](#0-create-a-service---post-graphscreateservice)
  - [0.1 service definition](#01-service-definition)
- [1. Create a Label - `POST /graphs/createLabel`](#1-create-a-label---post-graphscreatelabel)
  - [1.1 label definition](#11-label-definition)
  - [1.2 label example](#12-label-example)
  - [1.3 Add extra props on label.](#13-add-extra-props-on-label)
  - [1.4 Consistency level.](#14-consistency-level)
  - [1.4 (Optionally) Add Extra Indexes - `POST /graphs/addIndex`](#14-optionally-add-extra-indexes---post-graphsaddindex)
- [2. Create ServiceColumn(Optional) - `POST /graphs/createServiceColumn`](#2-create-servicecolumnoptional---post-graphscreateservicecolumn)
  - [2.1 ServiceColumn definition](#21-servicecolumn-definition)
  - [2.2 examples](#22-examples)
- [3. Insert and Manipulate Edges](#3-insert-and-manipulate-edges)
  - [Edge Operations](#edge-operations)
    - [strong consistency](#strong-consistency)
      - [1. Insert - `POST /graphs/edges/insert`](#1-insert---post-graphsedgesinsert)
      - [2. delete -`POST /graphs/edges/delete`](#2-delete--post-graphsedgesdelete)
      - [3. update -`POST /graphs/edges/update`](#3-update--post-graphsedgesupdate)
      - [4. increment -`POST /graphs/edges/increment`](#4-increment--post-graphsedgesincrement)
      - [5. deleteAll - `POST /graphs/edges/deleteAll`](#5-deleteall---post-graphsedgesdeleteall)
    - [weak consistency](#weak-consistency)
      - [1. Insert - `POST /graphs/edges/insert`](#1-insert---post-graphsedgesinsert-1)
      - [2. delete -`POST /graphs/edges/delete`](#2-delete--post-graphsedgesdelete-1)
      - [3. update -`POST /graphs/edges/update`](#3-update--post-graphsedgesupdate-1)
      - [4. increment -`POST /graphs/edges/increment`](#4-increment--post-graphsedgesincrement-1)
      - [5. deleteAll - `POST /graphs/edges/deleteAll`](#5-deleteall---post-graphsedgesdeleteall-1)
- [4. (Optionally) Insert and Manipulate Vertices](#4-optionally-insert-and-manipulate-vertices)
    - [1. Insert - `POST /graphs/vertices/insert/:serviceName/:columnName`](#1-insert---post-graphsverticesinsertservicenamecolumnname)
    - [2. delete - `POST /graphs/vertices/delete/:serviceName/:columnName`](#2-delete---post-graphsverticesdeleteservicenamecolumnname)
    - [3. deleteAll - `POST /graphs/vertices/deleteAll/:serviceName/:columnName`](#3-deleteall---post-graphsverticesdeleteservicenamecolumnname)
    - [3. update - `POST /graphs/vertices/update/:serviceName/:columnName`](#3-update---post-graphsverticesupdateservicenamecolumnname)
    - [4. increment](#4-increment)
- [5. Query](#5-query)
  - [1. Definition](#1-definition)
  - [2. Query API](#2-query-api)
    - [2.1. Edge Queries](#21-edge-queries)
      - [1. POST /graphs/getEdges](#1-post-graphsgetedges)
      - [2. POST /graphs/checkEdges](#2-post-graphscheckedges)
    - [2.2 getEdges Examples](#22-getedges-examples)
      - [1. duplicate policy](#1-duplicate-policy)
      - [2. select option example](#2-select-option-example)
      - [3. groupBy option example](#3-groupby-option-example)
      - [4. filterOut option example](#4-filterout-option-example)
      - [5. nextStepLimit example](#5-nextsteplimit-example)
      - [6. nextStepThreshold example](#6-nextstepthreshold-example)
      - [7. transform example](#7-transform-example)
      - [8. 2 step traverse example](#8-2-step-traverse-example)
      - [9. 3 step traverse example](#9-3-step-traverse-example)
      - [10. more examples](#10-more-examples)
    - [2.2. Vertex Queries](#22-vertex-queries)
      - [1. POST /graphs/getVertices](#1-post-graphsgetvertices)
- [6. Bulk Loading](#6-bulk-loading)
    - [Edge Format](#edge-format)
    - [Vertex Format](#vertex-format)
  - [Build](#build)
  - [Source Data Storage Options](#source-data-storage-options)
    - [1. When the source data is in HDFS.](#1-when-the-source-data-is-in-hdfs)
    - [2. When the source data is in Kafka.](#2-when-the-source-data-is-in-kafka)
    - [3. online migration](#3-online-migration)
- [7. Benchmark](#7-benchmark)
  - [Test data](#test-data)
    - [1. friend of friend](#1-friend-of-friend)
    - [2. friends](#2-friends)
  - [new benchmark (asynchbase)](#new-benchmark-asynchbase)
    - [1. one step query](#1-one-step-query)
    - [2. two step query](#2-two-step-query)
    - [3. three step query](#3-three-step-query)
- [8. Resources](#8-resources)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->



Getting Started
-------------

S2Graph consists of multiple projects.

1. **S2Core**: core library for common classes to store and retrieve data as edge/vertex. 
2. **root project**: Play rest server that provide rest APIs.
3. **spark**: spark related common classes. 
4. **loader**: spark jobs that consume events from Kafka to HBase using S2Core library. also contains migration kit from hdfs to s2graph.
5. **asynchbase**: This is fork from https://github.com/OpenTSDB/asynchbase. we add few functionalities on GetRequest. all theses are heavily relies on pull requests which not have been merged on original project yet. 
	6. rpcTimeout
	7. setFilter
	8. column pagination
	9. retryAttempCount
	10. timestamp filtering


----------

to getup and running following is required. 

1. [Apache HBase](http://hbase.apache.org/) setup. 
	2.  `brew install hadoop` and `brew install hbase` if you are on mac.
	3. otherwise checkout [reference](http://hbase.apache.org/book.html#quickstart) for how to setup hbase.
	4. note that currently we support latest stable version of apache **hbase 1.0.1 with apache hadoop version 2.7.0**. if you are using cdh, then you can checkout our **feature/cdh5.3.0**. we are working on providing profile on hbase/hadoop version soon.
2. s2graph store metadata in mysql currently, so you need to create database in mysql. run setup script s2core/migrate/mysql/schema.sql

then compile rest project
```
sbt compile
```


now you are ready to run s2graph.
```
sbt run
```

we provide simple script under script/test.sh only for checking if everything setup property.

```
sh script/test.sh
```

finally join the [mailing list](https://groups.google.com/forum/#!forum/s2graph)



The Data Model
--------------

There are four important abstractions that define the data model used throughout s2graph: services, columns, labels and properties.

**Services**, the top level abstraction, are like databases in traditional RDBMS in which all data are contained. A service usually represents one of the company's real services and is named accordingly, e.g. `"KakaoTalk"`, `"KakaoStory"`.

**Columns** define the type of vertices and a service can have multiple columns. For example, service `"KakaoMusic"` can have columns `"user_id"` and `"track_id"`. While columns can be compared to tables in traditional RDBMS in a sense, labels are the primary abstraction for representing schemas and columns are usually referenced only using their names.

**Labels**, represent relations between two columns, therefore representing the type of edges. The two columns can be the same, e.g. for a label representing friendships in an SNS, the two column will both be `"user_id"` of the service. There can be labels connecting two columns from two different services; for example, one can create a label that stores all events where KakaoStory posts are shared to KakaoTalk.

**Properties**, are metadata linked to vertices or edges that can be queried upon later. For vertices representing KakaoTalk users, `estimated_birth_year` is a possible property, and for edges representing similar KakaoMusic songs their `cosine_similarity` can be a property.

Using these abstractions, a unique vertex can be identified with its `(service, column, vertex id)`, and a unique edge can be identified with its `(service, label, source vertex id, target vertex id)`. Additional information on edges and vertices are stored within their own properties.


REST API Glossary
-----------------

The following is a non-exhaustive list of commonly used s2graph APIs and their examples. The full list of the latest REST API can be found in [the routes file](conf/routes).

## 0. Create a Service - `POST /graphs/createService`  ##


see following to see what you can set with this API.

### 0.1 service definition
To create a Service, the following fields needs to be specified in the request.

|field name |  definition | data type |  example | note |
|:------- | --- |:----: | --- | :-----|
| **serviceName** | name of user defined namespace. | string | "talk_friendship"| required. |
| cluster | zookeeper quorum address for your cluster.| string | "abc.com:2181,abd.com:2181" | optional. <br>default value is "hbase.zookeeper.quorum" on your application.conf. if there is no value for "hbase.zookeeper.quorum" is defined on application.conf, then default value is "localhost" |
| hTableName | physical HBase table name.|string| "test"| optional. <br> default is serviceName-#{phase}. <br> phase is either dev/real/alpha/sandbox |
| hTableTTL | global time to keep the data alive. | integer | 86000 | optional. default is infinite.
| preSplitSize | ratio for number of pre split for HBase table. numOfRegionServer x this number will decide exact pre-split size.| integer|1|optional. <br> default is 0(no pre-split). if you set this to 1, then s2graph will pre-split your table with 1 x **numOfRegionServers** |

Service is the top level abstraction in s2graph which can be considered like a database in RDBMS. You can create a service using this API:

```
curl -XPOST localhost:9000/graphs/createService -H 'Content-Type: Application/json' -d '
{"serviceName": "s2graph", "cluster": "address for zookeeper", "hTableName": "hbase table name", "hTableTTL": 86000, "preSplitSize": # of pre split}
'
```
>note that optional value for your service is only advanced users only. stick to default if you don`t know what you are doing.

You can also look up all labels corresponding to a service.

```
curl -XGET localhost:9000/graphs/getLabels/:serviceName
```







## 1. Create a Label - `POST /graphs/createLabel` ##


----------


A label represents a relation between two columns, and plays a role like a table in RDBMS since labels contain the schema information, i.e. what type of data will be collected and what among them needs to be indexed for efficient retrieval. In most scenario, defining a schema on vertices is pretty straightforward but defining a schema on edges requires a little effort. Think about queries you will need first, and then model user's actions/relations as **edges** to design a label.

### 1.1 label definition
To create a Label, the following fields needs to be specified in the request.

|field name |  definition | data type |  example | note |
|:------- | --- |:----: | --- | :-----|
| **label** | name of this relation; be specific. | string | "talk_friendship"| required. |
| srcServiceName | source column's service | string | "kakaotalk" | required. |
| srcColumnName | source column's name |string| "user_id"|required. |
| srcColumnType | source column's data type | long/integer/string|"string"|required.|
| tgtServiceName | target column's service | string | "kakaotalk"/"kakaoagit" | same as srcServiceName when not specified
| tgtColumnName | target column's name |string|"item_id"|required.|
| tgtColumnType | target column's data type | long/integer/string | "long" | required. |
| isDirected | if this label is directed or undirected | true/false | true/false | default true |
| **serviceName** | which service this label is belongs to. | string |s2graph |default tgtServiceName
| hTableName | if this label need special usecase(such as batch upload), own hbase table name can be used. | string | s2graph-batch | default use service`s hTableName. <br> note that this is optional. |
| hTableTTL | time to data keep alive. | integer |   86000 | default use service`s hTableTTL. <br> note that this is optional. |
| consistencyLevel | if this is strong, only one edge between same from/to can be made. otherwise(weak) multiple edges with same from/to can be exist. | string | strong/weak | default weak |
|**indices** | see below |
|**props** | see below |

Most important elements for label is their **indices** and **props**

All of graph element including Vertex and Edge has their properties. single property is defined as following. property is simple key, value map on Edge and Vertex.
```
{
    "name": "name of property",
    "dataType": "data type of property value",
    "defaultValue": "default value in string"
}
```

a simple example for props would look like this
```
[
    {"name": "play_count", "defaultValue": 0, "dataType": "integer"},
    {"name": "is_hidden","defaultValue": false,"dataType": "boolean"},
    {"name": "category","defaultValue": "jazz","dataType": "string"},
    {"name": "score","defaultValue": 0,"dataType": "float"}
]
```

note that property value type should be **numeric**(byte, short, integer, float, double) or **boolean** or **string**.

**indices** define indices for this label

first index in indices is primary (like `PRIMARY INDEX idx_xxx`(`p1, p2`) in RDBMS).

s2graph will automatically keep edges sorted according to this index.

other's for multiple ordering on edges.(like `ALTER TABLE ADD INDEX idx_xxx`(`p2, p1`) in RDBMS).

**props** define meta datas that will not be affect the order of edges. 
 
One last thing to note here is that s2graph reserved following property names. user can`t create following property name but they can use as it is provided by default.

>1. **_timestamp** is reserved for system wise timestamp. this can be interpreted as last_modified_at
>2. **_from** is reserved for label`s start vertex.
>3. **_to** is reserved for 

### 1.2 label example

Here is example that creates a label named `user_article_liked` label between column `user_id` of service `s2graph` and column `article_id` of service `s2graph_news`. Note that the default indexed property `_timestamp` will be created since the `indexedProps` field is empty.

```
curl -XPOST localhost:9000/graphs/createLabel -H 'Content-Type: Application/json' -d '
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
}
'
```

this label will keep edges ordered according to edge`s index values in this case, latest like first. default ordering is latest first which many application naturally want.


Here is another example that creates a label named `friends`, which represents the friend relation between users in `s2graph` service. In this case with higher affinity_score comes first and if affinity_score is ties, then latest friend comes first. `friends` label will belongs to `s2graph` service.

```
curl -XPOST localhost:9000/graphs/createLabel -H 'Content-Type: Application/json' -d '
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
}
'
```

s2graph support **multiple index** on label which means we can add other ordering option for edges with this label.

```
# with exists props
curl -XPOST localhost:9000/graphs/addIndex -H 'Content-Type: Application/json' -d '
{
    "label": "friends",
    "indices": [
        {"name": "idx_3rd", "propNames": ["is_blocked", "_timestamp"]}
    ]
}
'
```

To get information on label, just use following.

```
curl -XGET localhost:9000/graphs/getLabel/friends
```

You can also delete a label using the following API.

```
curl -XPUT localhost:9000/graphs/deleteLabel/friends
```

Update is not supported, so just delete label and re-create it.



### 1.3 Add extra props on label.

To add a new property, use the following API:

```
curl -XPOST localhost:9000/graphs/addProp/graph_test -H 'Content-Type: Application/json' -d '
{"name": "is_blocked", "defaultValue": false, "dataType": "boolean"}
'
```

### 1.4 Consistency level.
One last important constraint on label is **consistency level**.

>**This define how to store edges on storage level. note that query is completely independent with this.**

To explain consistency, s2graph defined edge uniquely with their (from, label, to) triple. s2graph call this triple as unique edge key.

following example is used to explain differences between strong/weak consistency level.
> ```
> 1418950524721	insert	e	1 	101	graph_test	{"weight": 10} = (1, graph_test, 101)
> 1418950524723	insert	e	1	101	graph_test	{"weight": 20} = (1, graph_test, 101)
> ```

currently there are two consistency level 


**1. strong**
>make sure there is **only one edge stored in storage** between same edge key(**(1, graph_test, 101)** above).
>with strong consistency level, last command overwrite previous command. 

**2. weak**
>no consistency check on unique edge key. above example yield **two different edge stored in storage** with different timestamp and weight value.

for example, with each configuration, following edges will be stored.

assumes that only timestamp is used as index prop and user inserts following.
```
u1 -> (t1, v1)
u1 -> (t2, v2)
u1 -> (t3, v2)
u1 -> (t4, v1)
```

with strong consistencyLevel following is what to be stored.
```
u1 -> (t4, v1), (t3, v2)
```
note that u1 -> (t1, v1), (t2, v2) are not exist.

with weak consistencyLevel.
```
u1 -> (t4, v1), (t3, v2), (t2, v2), (t1, v1)
```

Reason weak consistency is default.

> most case edges related to user`s activity should use **weak** consistencyLevel since there will be **no concurrent update on same edges**. strong consistencyLevel is only for edges expecting many concurrent updates.


Consistency level also determine how edges will be stored in storage when command is delivered reversely by their timestamp.

with strong consistencyLevel following is guaranteed.

natural event on (1, graph_test, 101) unique edge key is following.
```
1418950524721	insert	e	1	101	graph_test	{"is_blocked": false}
1418950524722	delete	e	1	101	graph_test
1418950524723	insert	e	1	101	graph_test	{"is_hidden": false, "weight": 10}
1418950524724	update	e	1	101	graph_test	{"time": 1, "weight": -10}
1418950524726	update	e	1	101	graph_test	{"is_blocked": true}
```

even if above commands arrive in not in order, strong consistency make sure same eventual state on (1, graph_test, 101).
```
1418950524726	update	e	1	101	graph_test	{"is_blocked": true}
1418950524723	insert	e	1	101	graph_test	{"is_hidden": false, "weight": 10}
1418950524722	delete	e	1	101	graph_test
1418950524721	insert	e	1	101	graph_test	{"is_blocked": false}
1418950524724	update	e	1	101	graph_test	{"time": 1, "weight": -10}
```

There are many cases that commands arrive in not in order.
>1. client servers are distributed and each client issue command asynchronously. 
>2. client servers are distributed and grouped commands.
>3. by using kafka queue, global ordering or message is not guaranteed.


Following is what s2graph do to make strong consistency level.
```
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
```


>**Limitation**
>Since we write our data to HBase asynchronously, there is no consistency guarantee on same edge within our flushInterval(1 seconds).



### 1.4 (Optionally) Add Extra Indexes - `POST /graphs/addIndex` ###


----------


A label can have multiple indexed properties, or (for brevity) indexes. When queried, returned edges' order is determined according to indexes, indexes essentially defines what will be included in the **topK** query.

> Edge retrieval queries in s2graph by default returns **topK** edges. Clients must issue another query to fetch the next K edges, i.e., **topK ~ 2topK**.

Internally, s2graph stores edges sorted according to the indexes in order to limit the number of edges to fetch in one query. If no ordering is given, s2graph will use the **timestamp** as an index, thus resulting in the most recent data.

> It is impossible to fetch millions of edges and sort them on-line to get topK in less than a second. s2graph uses vertex-centric indexes to avoid this.
>
> **using vertex-centric index, having millions of edges is fine as long as the topK value is reasonable (~ 1K)**
> **Note that indexes must be created before putting any data on this label** (just like RDBMS).

New indexes can be dynamically added, but it will not be applied to existing data(planned in future versions). **the number of indexes on a label is currently limited to 8.**

The following is an example of adding `play_count` to a label named `graph_test`.

```
// add prop first
curl -XPOST localhost:9000/graphs/addProp/graph_test -H 'Content-Type: Application/json' -d '
  { "name": "play_count", "defaultValue": 0, "dataType": "integer" }
'

// and then add index
curl -XPOST localhost:9000/graphs/addIndex -H 'Content-Type: Application/json' -d '
{
    "label": "graph_test",
    "indices": [ 
        { name: "idx_play_count", propNames: ["play-count"] } 
    ] 
}
'
```

## 2. Create ServiceColumn(Optional) - `POST /graphs/createServiceColumn` ##
----------
A ServiceColumn represents object and plays a role like a single table in RDBMS. 

>**Note: if you only need vertex id, then you don`t need to create vertex explicitly. when you create label, s2graph create vertex with empty properties according to label schema.**

### 2.1 ServiceColumn definition

| field name |  definition | data type |  note | example |
|:------- | --- |:----: | --- | :-----|
| serviceName | which service this vertex belongs to | string | required. think this as database in RDBMS | kakaotalk |
| columnName | what this vertex`s id is | string | required. think this as primary key in RDBMS table | talk_user_id |
| props | optional properties on vertex | json array of json dictionary | optional. think this as columns in RDBMS table | see examples |

### 2.2 examples
This is simple example to show how to define vertex schema and insert/select vertices.
```
curl -XPOST localhost:9000/graphs/createServiceColumn -H 'Content-Type: Application/json' -d ' 
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
}
' 
```

user can get information on vertex schema as following.

```
curl -XGET localhost:9000/graphs/getServiceColumn/s2graph/user_id 
```
This will give all properties on serviceName `s2graph` and columnName `user_id` serviceColumn.

user can also add more properties on vertex as following.

```
curl -XPOST localhost:9000/graphs/addServiceColumnProps/s2graph/user_id -H 'Content-Type: Application/json' -d '
[
	{"name": "home_address", "defaultValue": "korea", "dataType": "string"}
]
'
```

you can insert vertex data as following.
```
curl -XPOST localhost:9000/graphs/vertices/insert/s2graph/user_id -H 'Content-Type: Application/json' -d '
[
  {"id":1,"props":{"is_active":true}, "timestamp":1417616431},
  {"id":2,"props":{},"timestamp":1417616431}
]
'
```
 
finally you can query your vertex as following.
```
curl -XPOST localhost:9000/graphs/getVertices -H 'Content-Type: Application/json' -d '
[
	{"serviceName": "s2graph", "columnName": "user_id", "ids": [1, 2, 3]}
]
'
```

##3. Insert and Manipulate Edges ##


----------


An **edge** represents a relation between two vertices, with properties according to the schema defined in its label. The following fields need to be specified when inserting an edge, and are returned when queried on edges.

| field name |  definition | data type |  note | example |
|:------- | --- |:----: | --- | :-----|
| **timestamp** | when this request is issued. | long | required. in **millis** since the epoch. It is important to use millis, since TTL support is in millis.  | 1430116731156 |
| operation |insert/delete/update/increment | string | required only for bulk operation; aliases are insert: i, delete:d, update: u, increment: in, default is insert.| "i", "insert" |
| from |  Id of start vertex. |  long/string  | required. prefer long if possible. **maximum string bytes length < 249** |1|
| to | Id of end vertex. |  long/string | required. prefer long if possible. **maximum string bytes length < 249** |101|
| label | name the corresponding label  | string | required. |"graph_test"|
| direction | direction of this relation, one of **out/in/undirected** | string | required. alias are out: o, in: i, undirected: u| "out" |
| props | extra properties of this edge. | json dictionary | required. **all indexed properties should be present, otherwise the default values will be added. Non-indexed properties can also be present** | {"timestamp": 1417616431, "affinity_score":10, "is_hidden": false, "is_valid": true}|


### Edge Operations ###

s2graph provide 5 different operations on edge.

1. insert: create new edge. 
2. delete: delete existing edge.
3. update: update existing edge`s state.
4. increment: increment existing edge`s state.
5. deleteAll: delete all adjacent edges from certain starting vertex.(only for strong consistency)

edge operations behave differently with regard to their label`s consistencyLevel.

for explanation, consider following test cases.

create 2 different label each for strong/weak consistencyLevel.

1. s2graph_label_test(strong)
2. s2graph_label_test_weak(weak)


then insert 2 test cases.

**strong consistency**
```
curl -XPOST localhost:9000/graphs/edges/insert -H 'Content-Type: Application/json' -d '
[
	{"timestamp": 1, "from": 101, "to": 10, "label": "s2graph_label_test", "props": {"time": 0}},
	{"timestamp": 2, "from": 101, "to": 10, "label": "s2graph_label_test", "props": {"time": -10}},
	{"timestamp": 3, "from": 101, "to": 10, "label": "s2graph_label_test", "props": {"time": -30}}
]
'
```

note that only one edge exist between (101, 10, s2graph_label_test, out) since label s2graph_label_test is strong consistency.
```
{
    "size": 1,
    "degrees": [
        {
            "from": 101,
            "label": "s2graph_label_test",
            "direction": "out",
            "_degree": 1
        }
    ],
    "results": [
        {
            "cacheRemain": -20,
            "from": 101,
            "to": 10,
            "label": "s2graph_label_test",
            "direction": "out",
            "_timestamp": 3,
            "timestamp": 3,
            "score": 1,
            "props": {
                "_timestamp": 3,
                "time": -30,
                "weight": 0,
                "is_hidden": false,
                "is_blocked": false
            }
        }
    ],
    "impressionId": -1650835965
}
```


**weak consistency**
```
curl -XPOST localhost:9000/graphs/edges/insert -H 'Content-Type: Application/json' -d '
[
	{"timestamp": 1, "from": 101, "to": 10, "label": "s2graph_label_test_weak", "props": {"time": 0}},
	{"timestamp": 2, "from": 101, "to": 10, "label": "s2graph_label_test_weak", "props": {"time": -10}},
	{"timestamp": 3, "from": 101, "to": 10, "label": "s2graph_label_test_weak", "props": {"time": -30}}
]
'
```

note that there are **3 edges** between (101, 10, s2graph_label_test_weak, out).
```
{
    "size": 3,
    "degrees": [
        {
            "from": 101,
            "label": "s2graph_label_test_weak",
            "direction": "out",
            "_degree": 3
        }
    ],
    "results": [
        {
            "cacheRemain": -148,
            "from": 101,
            "to": "10",
            "label": "s2graph_label_test_weak",
            "direction": "out",
            "_timestamp": 3,
            "timestamp": 3,
            "score": 1,
            "props": {
                "_timestamp": 3,
                "time": -30,
                "weight": 0,
                "is_hidden": false,
                "is_blocked": false
            }
        },
        {
            "cacheRemain": -148,
            "from": 101,
            "to": "10",
            "label": "s2graph_label_test_weak",
            "direction": "out",
            "_timestamp": 2,
            "timestamp": 2,
            "score": 1,
            "props": {
                "_timestamp": 2,
                "time": -10,
                "weight": 0,
                "is_hidden": false,
                "is_blocked": false
            }
        },
        {
            "cacheRemain": -148,
            "from": 101,
            "to": "10",
            "label": "s2graph_label_test_weak",
            "direction": "out",
            "_timestamp": 1,
            "timestamp": 1,
            "score": 1,
            "props": {
                "_timestamp": 1,
                "time": 0,
                "weight": 0,
                "is_hidden": false,
                "is_blocked": false
            }
        }
    ],
    "impressionId": 1972178414
}
```



#### strong consistency ####
##### 1. Insert - `POST /graphs/edges/insert` #####

unique edge is defined by (from, to, label, direction). 
for insert operation, s2graph first check if there exist edge with same (from, to, label, direction) then if there is an existing edge, then insert works as **update**. see above example.

##### 2. delete -`POST /graphs/edges/delete` ##### 
s2graph looks for unique edge with (from, to, label, direction) then delete them if deleting edge is valid. 
note that delete request has **strictly larger timestamp** than existing edge. since s2graph check validation on delete request, if existing edge has large timestamp than current delete request, then current delete request will be ignored. also note that props on delete request is not necessary and will be ignored when label is strong consistency since it is safe to assume there is only one edge with edge`s unique id(from, to, label, direction).

```
curl -XPOST localhost:9000/graphs/edges/delete -H 'Content-Type: Application/json' -d '
[
	{"timestamp": 10, "from": 101, "to": 10, "label": "s2graph_label_test"}
]
'
```
##### 3. update -`POST /graphs/edges/update` ##### 
works like insert with strong consistency level. 
```
curl -XPOST localhost:9000/graphs/edges/update -H 'Content-Type: Application/json' -d '
[
	{"timestamp": 10, "from": 101, "to": 10, "label": "s2graph_label_test", "props": {"time": 100, "weight": -10}}
]
'
```
##### 4. increment -`POST /graphs/edges/increment` ##### 
works like update. only difference is increment call don`t return old value but incremented value.
```
curl -XPOST localhost:9000/graphs/edges/increment -H 'Content-Type: Application/json' -d '
[
	{"timestamp": 10, "from": 101, "to": 10, "label": "s2graph_label_test", "props": {"time": 100, "weight": -10}}
]
'
```
##### 5. deleteAll - `POST /graphs/edges/deleteAll` ##### 
delete all adjacency edges that start from starting vertex.
following operation will first fetch edges start from 101 then delete all edges. 
**note that not only all out going edges from 101 and but also incoming edges to 101 will be deleted.**

```
curl -XPOST localhost:9000/graphs/edges/deleteAll -H 'Content-Type: Application/json' -d '
[
  {"ids" : [101], "label":"s2graph_label_test", "direction": "out", "timestamp":1417616441}
]
'
```

#### weak consistency ####
##### 1. Insert - `POST /graphs/edges/insert` #####
s2graph **do not look for** unique edge defined by (from, to, label, direction).
it simply make new edge for user`s request. no read and no consistency check. note that this difference make multiple edge with same (from, to, label, direction) exist. see above example.

##### 2. delete -`POST /graphs/edges/delete` ##### 
to delete edges with weak consistency, first thing to do is fetching existing edges.
with getEdges query result json, extract "results" part in it, then fire /graphs/edges/delete with "results" part json.
note that request body json is copied from "**results**" part in /graphs/getEdges.
```
curl -XPOST localhost:9000/graphs/edges/delete -H 'Content-Type: Application/json' -d '
[
    {
        "cacheRemain": -148,
        "from": 101,
        "to": "10",
        "label": "s2graph_label_test_weak",
        "direction": "out",
        "_timestamp": 3,
        "timestamp": 3,
        "score": 1,
        "props": {
            "_timestamp": 3,
            "time": -30,
            "weight": 0,
            "is_hidden": false,
            "is_blocked": false
        }
    },
    {
        "cacheRemain": -148,
        "from": 101,
        "to": "10",
        "label": "s2graph_label_test_weak",
        "direction": "out",
        "_timestamp": 2,
        "timestamp": 2,
        "score": 1,
        "props": {
            "_timestamp": 2,
            "time": -10,
            "weight": 0,
            "is_hidden": false,
            "is_blocked": false
        }
    },
    {
        "cacheRemain": -148,
        "from": 101,
        "to": "10",
        "label": "s2graph_label_test_weak",
        "direction": "out",
        "_timestamp": 1,
        "timestamp": 1,
        "score": 1,
        "props": {
            "_timestamp": 1,
            "time": 0,
            "weight": 0,
            "is_hidden": false,
            "is_blocked": false
        }
    }
]
'
```
##### 3. update -`POST /graphs/edges/update` ##### 
like insert, s2graph **do not look check** uniqueness. all responsibility is on users. like delete, update require fetch existing edges first. after fetching edges, update props then issue update request.

##### 4. increment -`POST /graphs/edges/increment` ##### 
like update, s2graph **do not look check** uniqueness. all responsibility is on users. like delete, update require fetch existing edges first. after fetching edges, update props then issue increment request.

##### 5. deleteAll - `POST /graphs/edges/deleteAll` ##### 
not supported with weak consistency. 
like update, fetch all edges then issue delete individually on fetched edges. 

## 4. (Optionally) Insert and Manipulate Vertices ##



Vertices are the two ends that an edge is connecting, and correspond to a column defined for a service. In case you need to store some metadata corresponding to vertices and make queries regarding them, you can insert and manipulate vertices rather than edges.

Unlike edges and their labels, properties on vertices are not indexed and do not require a predefined schema nor default values. The following fields are used when operating on vertices. 

| field name |  definition | data type |  note | example |
|:------- | --- |:----: | --- | :-----|
| timestamp |  | long | required. in seconds since the epoch | 1417616431 |
| operation | the operation to perform; one of insert, delete, update, increment | string | required only for bulk operations; alias are insert: i, delete:d, update: u, increment: in, default is insert.| "i", "insert" |
| **serviceName** | corresponding service's name | "string" | required. | "kakaotalk"/"kakaogroup"|
| **columnName** | corresponding column's name |  string  | required. |"xxx_service_ user_id"|
| id     | a unique identifier of this vertex |  long/string | required. prefer long if possible.|101|
| **props** | extra properties of this vertex. | json dictionary | required.  | {"is_active_user": true, "age":10, "gender": "F", "country_iso": "kr"}|



----------

#### 1. Insert - `POST /graphs/vertices/insert/:serviceName/:columnName` ####

```
curl -XPOST localhost:9000/graphs/vertices/insert/s2graph/account_id -H 'Content-Type: Application/json' -d '
[
  {"id":1,"props":{"is_active":true, "talk_user_id":10},"timestamp":1417616431},
  {"id":2,"props":{"is_active":true, "talk_user_id":12},"timestamp":1417616431},
  {"id":3,"props":{"is_active":false, "talk_user_id":13},"timestamp":1417616431},
  {"id":4,"props":{"is_active":true, "talk_user_id":14},"timestamp":1417616431},
  {"id":5,"props":{"is_active":true, "talk_user_id":15},"timestamp":1417616431}
]
'
```

#### 2. delete - `POST /graphs/vertices/delete/:serviceName/:columnName` ####

This operation will delete only the vertex data of a specified column and will **not** delete all edges connected to those vertices. 

**Important notes**
>**This means that edges returned by a query can contain deleted vertices. Clients need to check if those vertices are valid.**

#### 3. deleteAll - `POST /graphs/vertices/deleteAll/:serviceName/:columnName` ####

This operation will delete all vertex data of a specified column and also delete all edges that are connected to those vertices. Example:

```
curl -XPOST localhost:9000/graphs/vertices/deleteAll/s2graph/account_id -H 'Content-Type: Application/json' -d '
[{"id": 1, "timestamp": 193829198}]
'
```

This is an extremely expensive operation; The following is a pseudocode showing how this operation works:

```
vertices = vertex list to delete
for vertex in vertices
	labals = fetch all labels that this vertex is included.
	for label in labels
		for index in label.indices
			edges = G.read with limit 50K
			for edge in edges
				edge.delete
```

The total complexity is O(L * L.I) reads + O(L * L.I * 50K) writes in the worst case. **If a vertex to delete has more than 50K edges, the delete operation will not be consistent.** 



#### 3. update - `POST /graphs/vertices/update/:serviceName/:columnName` ####

The update operation on vertices uses the same parameters as in the insert operation.

#### 4. increment ####

Not yet implemented; stay tuned.


## 5. Query ##



### 1. Definition ###


----------


Once you have your graph data uploaded to s2graph, you can traverse your graph using our REST APIs. Queries contain the vertex to start traversing, and list of labels paired with filters and scoring weights used during the traversal. Query requests are structures as follows:

| field name |  definition | data type |  note | example |
|:------- | --- |:----: | --- | :-----|
| srcVertices | vertices to start traversing. |json array of json dictionary specifying each vertex, with "serviceName", "columnName", "id" fields. | required. | `[{"serviceName": "kakao", "columnName": "account_id", "id":1}]` |
|**steps**| list of steps for traversing. | json array of steps | explained below| ```[[{"label": "graph_test", "direction": "out", "limit": 100, "scoring":{"time": 0, "weight": 1}}]] ``` |
|removeCycle| when traverse to next step, don`t traverse already visited vertices| true/false. default is true | already visited is defined by following(label, vertex). <br> so if steps are friend -> friend, then remove second depth friends if they exist in first depth friends | |
|select| which field on edge to include in result json | json array of string | | ["label", "to", "from"] |
|groupBy | how to group by results | json array of string | | ["to"] |
|filterOut | filtering out query which will be run concurrently then filter out |  | another query json |


**step**: Each step define what to traverse in a single hop on the graph. The first step has to be a direct neighbor of the starting vertices, the second step is a direct neighbor of vertices from the first step and so on. A step is specified with a list of **query param**s, hence the `steps` field of a query request becoming an array of arrays of dictionaries.

**step param**: 

| field name |  definition | data type |  note | example |
|:------- | --- |:----: | --- | :-----|
| weights | weight constant to multiply for labels in this step | json dictionay | optional | {"graph_test": 0.3, "graph_test2": 0.2} | 
| nextStepThreshold | score threshold for current step edges to pass to next step | double |  | |
| nextStepLimit | number of edges in current step to pass to next step | double | if this parameter is given, then sort current step by score and take topK, then start traverse next step|

**query param**: 

| field name |  definition | data type |  note | example |
|:------- | --- |:----: | --- | :-----|
| label | name of label to traverse.| string | required. must be an existing label. | "graph_test" |
| direction | in/out direction to traverse | string | optional, default out | "out" |
| limit | how many edges to fetch | int | optional, default 10 | 10 |
| offset | start position on this index | int | optional, default 0 | 50 |
| interval | the range to filter on indexed properties | json dict | optional | `{"from": {"time": 0, "weight": 1}, "to": {"time": 1, "weight": 15}}` |
| duration | time range | json dict | optional| `{"from": 1407616431, "to": 1417616431}` |
| scoring | a mapping from indexed properties' names to their weights <br> the weighted sum of property values will be the final score. | json dict| optional | `{"time": 1, "weight": 2}` |
| where | filter condition(like sql`s where clause). <br> logical operation(**and/or**) is supported and each condition can have exact equal(=), sets(in), and range(between x and y). <br> **do not use any quotes for string type** | string | optional | ex) "((_from = 123 and _to = abcd) or gender = M) and is_hidden = false and weight between 1 and 10 or time in (1, 2, 3)". <br> note that it only support **long/string/boolean type**|
|outputField|replace edge`s to field with this field in props| string | optional | "outputField": "service_user_id". this change to field into props['service_user_id'] |
|exclude| decide if vertices that appear on this label and different labels in this step should be filtered out | boolean | optional, default false | true, exclude vertices that appear on this label and other labels in this step will be filtered out.|
|include | decide if vertices that appear on this label and different labels in this step should be remain in result. | boolean | optional, default false | | 
| duplicate | policy on how to deal with duplicate edges. <br> duplicate edges means edges with same (from, to, label, direction). | string <br> one of "first", "sum", "countSum", "raw" | optional, default "**first**" | "**first**" means only first occurrence of edge survive. <br> "**sum**" means sums up all scores of same edges but only one edge survive. <br>"**countSum**" means counts up occurrence of same edges but only one edge survive. <br>"**raw**" means same edges will be survived as they are. |
| rpcTimeout | timeout for this request | integer | optional, default 100ms | note: maximum value should be less than 1000ms |
| maxAttempt | how many times client will try to fetch result from HBase | integer | optional, default 1 | note: maximum value should be less than 5|
| _to | to vertex id | string | optional | note: use this to get a edge for certain vertex |
| threshold | score threshold for filtering out result edges | double | optional, default 0.0 | 
| transform | rules define how to transform _to field value on edge | json array of json array | optional, default [ ["_to"]] |


### 2. Query API ###

#### 2.1. Edge Queries ####
s2graph provide query DSL like sql. 
s2graph use getEdges to fetch data and traverse multiple steps, just like users use select query in mysql.

users have been struggled since query DSL can be very complex, so if you can`t find any idea even after reading through example query section, then give me issue specifying what is your use case.

##### 1. POST /graphs/getEdges #####
select all edges with given query.

##### 2. POST /graphs/checkEdges #####
return edge for given vertex pair only if edge exist.

#### 2.2 getEdges Examples ####

##### 1. duplicate policy #####
this is very basic query to fetch all adjacent edges from starting vertex.
```
curl -XPOST localhost:9000/graphs/getEdges -H 'Content-Type: Application/json' -d '
{
    
    "srcVertices": [
        {
            "serviceName": "s2graph",
            "columnName": "user_id_test",
            "id": 101
        }
    ],
    "steps": [
        {
            "step": [
                {
                    "label": "s2graph_label_test_weak",
                    "direction": "out",
                    "offset": 0,
                    "limit": 10, 
	                "duplicate": "raw"
                }
            ]
        }
    ]
}
'
```

**note "duplicate" field**. when consistency level is **weak and multiple edges** exist with same (from, to, label, direction), then query expect duplicate policy. s2graph provide 4 duplicate policies on edge.
>1. raw: return all edges.
>2. **first**: return only first edge if multiple edges exist. this is default
>3. countSum: return only one edge but return how many times same edge exist.
>4. sum: return only one edge but return sum of their score.


you can see with "raw" duplicate policy, there are actually 3 edges with same (from, to, label, direction).
```
{
    "size": 3,
    "degrees": [
        {
            "from": 101,
            "label": "s2graph_label_test_weak",
            "direction": "out",
            "_degree": 3
        }
    ],
    "results": [
        {
            "cacheRemain": -29,
            "from": 101,
            "to": "10",
            "label": "s2graph_label_test_weak",
            "direction": "out",
            "_timestamp": 6,
            "timestamp": 6,
            "score": 1,
            "props": {
                "_timestamp": 6,
                "time": -30,
                "weight": 0,
                "is_hidden": false,
                "is_blocked": false
            }
        },
        {
            "cacheRemain": -29,
            "from": 101,
            "to": "10",
            "label": "s2graph_label_test_weak",
            "direction": "out",
            "_timestamp": 5,
            "timestamp": 5,
            "score": 1,
            "props": {
                "_timestamp": 5,
                "time": -10,
                "weight": 0,
                "is_hidden": false,
                "is_blocked": false
            }
        },
        {
            "cacheRemain": -29,
            "from": 101,
            "to": "10",
            "label": "s2graph_label_test_weak",
            "direction": "out",
            "_timestamp": 4,
            "timestamp": 4,
            "score": 1,
            "props": {
                "_timestamp": 4,
                "time": 0,
                "weight": 0,
                "is_hidden": false,
                "is_blocked": false
            }
        }
    ],
    "impressionId": 1972178414
}
```

now "countSum" policy gives only one edge but with score 3.
```
{
    "size": 1,
    "degrees": [
        {
            "from": 101,
            "label": "s2graph_label_test_weak",
            "direction": "out",
            "_degree": 3
        }
    ],
    "results": [
        {
            "cacheRemain": -135,
            "from": 101,
            "to": "10",
            "label": "s2graph_label_test_weak",
            "direction": "out",
            "_timestamp": 4,
            "timestamp": 4,
            "score": 3,
            "props": {
                "_timestamp": 4,
                "time": 0,
                "weight": 0,
                "is_hidden": false,
                "is_blocked": false
            }
        }
    ],
    "impressionId": 1972178414
}
```

##### 2. select option example #####
continue with example 1, sometimes user only want few fields on edge. this can be done with select. 
```
{
    "select": ["from", "to", "label"],
    "srcVertices": [
        {
            "serviceName": "s2graph",
            "columnName": "user_id_test",
            "id": 101
        }
    ],
    "steps": [
        {
            "step": [
                {
                    "label": "s2graph_label_test_weak",
                    "direction": "out",
                    "offset": 0,
                    "limit": 10, 
	                "duplicate": "raw"
                }
            ]
        }
    ]
}
```

now user tell s2graph that only ["from", "to", "label"] fields are necessary so s2graph return only those fields on result json.

```
{
    "size": 3,
    "degrees": [
        {
            "from": 101,
            "label": "s2graph_label_test_weak",
            "direction": "out",
            "_degree": 3
        }
    ],
    "results": [
        {
            "from": 101,
            "to": "10",
            "label": "s2graph_label_test_weak"
        },
        {
            "from": 101,
            "to": "10",
            "label": "s2graph_label_test_weak"
        },
        {
            "from": 101,
            "to": "10",
            "label": "s2graph_label_test_weak"
        }
    ],
    "impressionId": 1972178414
}
```

default behavior of select option is empty array and if select option is empty, then all edge fields will be returned.

##### 3. groupBy option example #####

some times use want to group by result edges by their field.
```
{   
  	"select": ["from", "to", "label", "direction", "timestamp", "score", "time", "weight", "is_hidden", "is_blocked"],
    "groupBy": ["from", "to", "label"],
    "srcVertices": [
        {
            "serviceName": "s2graph",
            "columnName": "user_id_test",
            "id": 101
        }
    ],
    "steps": [
        {
            "step": [
                {
                    "label": "s2graph_label_test_weak",
                    "direction": "out",
                    "offset": 0,
                    "limit": 10, 
	                "duplicate": "raw"
                }
            ]
        }
    ]

}
```

now result json grouped all edges by their ["from", "to", "label"] fields.
```
{
    "size": 1,
    "results": [
        {
            "groupBy": {
                "from": 101,
                "to": "10",
                "label": "s2graph_label_test_weak"
            },
            "agg": [
                {
                    "from": 101,
                    "to": "10",
                    "label": "s2graph_label_test_weak",
                    "direction": "out",
                    "timestamp": 6,
                    "score": 1,
                    "props": {
                        "time": -30,
                        "weight": 0,
                        "is_hidden": false,
                        "is_blocked": false
                    }
                },
                {
                    "from": 101,
                    "to": "10",
                    "label": "s2graph_label_test_weak",
                    "direction": "out",
                    "timestamp": 5,
                    "score": 1,
                    "props": {
                        "time": -10,
                        "weight": 0,
                        "is_hidden": false,
                        "is_blocked": false
                    }
                },
                {
                    "from": 101,
                    "to": "10",
                    "label": "s2graph_label_test_weak",
                    "direction": "out",
                    "timestamp": 4,
                    "score": 1,
                    "props": {
                        "time": 0,
                        "weight": 0,
                        "is_hidden": false,
                        "is_blocked": false
                    }
                }
            ]
        }
    ],
    "impressionId": 1972178414
}
```
##### 4. filterOut option example #####
sometimes it is necessary run 2 query concurrently, then filter out result with other query result. 

```
{
    "filterOut": {
        "srcVertices": [
            {
                "serviceName": "s2graph",
                "columnName": "user_id_test",
                "id": 100
            }
        ],
        "steps": [
            {
                "step": [
                    {
                        "label": "s2graph_label_test_weak",
                        "direction": "out",
                        "offset": 0,
                        "limit": 10,
                        "duplicate": "raw"
                    }
                ]
            }
        ]
    },
    "srcVertices": [
        {
            "serviceName": "s2graph",
            "columnName": "user_id_test",
            "id": 101
        }
    ],
    "steps": [
        {
            "step": [
                {
                    "label": "s2graph_label_test_weak",
                    "direction": "out",
                    "offset": 0,
                    "limit": 10,
                    "duplicate": "raw"
                }
            ]
        }
    ]
}
```
s2graph run 2 concurrent query, one for main step, and other is filter out query. 
above example only show syntax, so here is more concrete examples.

```
{
  "filterOut": {
    "srcVertices": [
      {
        "columnName": "uuid",
        "id": "alec",
        "serviceName": "nachu"
      }
    ],
    "steps": [
      {
        "step": [
          {
            "direction": "out",
            "label": "nachu_user_view_news",
            "limit": 100,
            "offset": 0
          }
        ]
      }
    ]
  },
  "srcVertices": [
    {
      "columnName": "uuid",
      "id": "alec",
      "serviceName": "nachu"
    }
  ],
  "steps": [
    {
      "nextStepLimit": 10,
      "step": [
        {
          "direction": "out",
          "duplicate": "scoreSum",
          "label": "nachu_user_view_news",
          "limit": 100,
          "offset": 0,
          "timeDecay": {
            "decayRate": 0.1,
            "initial": 1,
            "timeUnit": 86000000
          }
        }
      ]
    },
    {
      "nextStepLimit": 10,
      "step": [
        {
          "label": "nachu_news_belongto_category",
          "limit": 1
        }
      ]
    },
    {
      "step": [
        {
          "direction": "in",
          "label": "nachu_news_belongto_category",
          "limit": 10
        }
      ]
    }
  ]
}
```

above main query will traverse news graph as following.
1. find out news list that user alec read.
2. find out categories for step 1`s news.
3. find out other news that is published in same category.

also concurrently, alec don`t want to get news recommendations that he already read. by using filterOut, client can add following step on result.

news that alec read
```
{
    "size": 5,
    "degrees": [
        {
            "from": "alec",
            "label": "nachu_user_view_news",
            "direction": "out",
            "_degree": 6
        }
    ],
    "results": [
        {
            "cacheRemain": -19,
            "from": "alec",
            "to": 20150803143507760,
            "label": "nachu_user_view_news",
            "direction": "out",
            "_timestamp": 1438591888454,
            "timestamp": 1438591888454,
            "score": 0.9342237306639056,
            "props": {
                "_timestamp": 1438591888454
            }
        },
        {
            "cacheRemain": -19,
            "from": "alec",
            "to": 20150803150406010,
            "label": "nachu_user_view_news",
            "direction": "out",
            "_timestamp": 1438591143640,
            "timestamp": 1438591143640,
            "score": 0.9333716513280771,
            "props": {
                "_timestamp": 1438591143640
            }
        },
        {
            "cacheRemain": -19,
            "from": "alec",
            "to": 20150803144908340,
            "label": "nachu_user_view_news",
            "direction": "out",
            "_timestamp": 1438581933262,
            "timestamp": 1438581933262,
            "score": 0.922898833570944,
            "props": {
                "_timestamp": 1438581933262
            }
        },
        {
            "cacheRemain": -19,
            "from": "alec",
            "to": 20150803124627492,
            "label": "nachu_user_view_news",
            "direction": "out",
            "_timestamp": 1438581485765,
            "timestamp": 1438581485765,
            "score": 0.9223930035297659,
            "props": {
                "_timestamp": 1438581485765
            }
        },
        {
            "cacheRemain": -19,
            "from": "alec",
            "to": 20150803113311090,
            "label": "nachu_user_view_news",
            "direction": "out",
            "_timestamp": 1438580536376,
            "timestamp": 1438580536376,
            "score": 0.9213207756669546,
            "props": {
                "_timestamp": 1438580536376
            }
        }
    ],
    "impressionId": 354266627
}
```

without filterOut
```
{
    "size": 2,
    "degrees": [
        {
            "from": 1028,
            "label": "nachu_news_belongto_category",
            "direction": "in",
            "_degree": 2
        }
    ],
    "results": [
        {
            "cacheRemain": -33,
            "from": 1028,
            "to": 20150803105805092,
            "label": "nachu_news_belongto_category",
            "direction": "in",
            "_timestamp": 1438590169146,
            "timestamp": 1438590169146,
            "score": 0.9342777143725886,
            "props": {
                "updateTime": 20150803172249144,
                "_timestamp": 1438590169146
            }
        },
        {
            "cacheRemain": -33,
            "from": 1028,
            "to": 20150803143507760,
            "label": "nachu_news_belongto_category",
            "direction": "in",
            "_timestamp": 1438581548486,
            "timestamp": 1438581548486,
            "score": 0.9342777143725886,
            "props": {
                "updateTime": 20150803145908490,
                "_timestamp": 1438581548486
            }
        }
    ],
    "impressionId": -14034523
}
```


with filterOut
```
{
    "size": 1,
    "degrees": [],
    "results": [
        {
            "cacheRemain": 85957406,
            "from": 1028,
            "to": 20150803105805092,
            "label": "nachu_news_belongto_category",
            "direction": "in",
            "_timestamp": 1438590169146,
            "timestamp": 1438590169146,
            "score": 0.9343106784173475,
            "props": {
                "updateTime": 20150803172249144,
                "_timestamp": 1438590169146
            }
        }
    ],
    "impressionId": -14034523
}
```

note that **20150803143507760** has been filtered out.


##### 5. nextStepLimit example #####
s2graph provide step level aggregation and users can decide topK on aggregated results.
back to s2graph_label_test_weak label, user may want to 


##### 6. nextStepThreshold example #####

##### 7. transform example #####
for traversing, s2graph use current step output edge`s to id(vertex id) for start vertexId on next step.
sometimes users want to keep traversing with current step output edge property value. 

below is result from example 1. 

```
{
    "size": 3,
    "degrees": [
        {
            "from": 101,
            "label": "s2graph_label_test_weak",
            "direction": "out",
            "_degree": 3
        }
    ],
    "results": [
        {
            "cacheRemain": -147,
            "from": 101,
            "to": "10",
            "label": "s2graph_label_test_weak",
            "direction": "out",
            "_timestamp": 6,
            "timestamp": 6,
            "score": 1,
            "props": {
                "_timestamp": 6,
                "time": -30,
                "weight": 0,
                "is_hidden": false,
                "is_blocked": false
            }
        },
        {
            "cacheRemain": -147,
            "from": 101,
            "to": "10",
            "label": "s2graph_label_test_weak",
            "direction": "out",
            "_timestamp": 5,
            "timestamp": 5,
            "score": 1,
            "props": {
                "_timestamp": 5,
                "time": -10,
                "weight": 0,
                "is_hidden": false,
                "is_blocked": false
            }
        },
        {
            "cacheRemain": -147,
            "from": 101,
            "to": "10",
            "label": "s2graph_label_test_weak",
            "direction": "out",
            "_timestamp": 4,
            "timestamp": 4,
            "score": 1,
            "props": {
                "_timestamp": 4,
                "time": 0,
                "weight": 0,
                "is_hidden": false,
                "is_blocked": false
            }
        }
    ],
    "impressionId": 1972178414
}
```

Here is how to use transform.

```
{
    "select": [],
    "srcVertices": [
        {
            "serviceName": "s2graph",
            "columnName": "user_id_test",
            "id": 101
        }
    ],
    "steps": [
        {
            "step": [
                {
                    "label": "s2graph_label_test_weak",
                    "direction": "out",
                    "offset": 0,
                    "limit": 10,
                    "duplicate": "raw",
                    "transform": [
                        ["_to"],
                        ["time.$", "time"]
                    ]
                }
            ]
        }
    ]
}
```


note that 3 edges become 6 edges since there are two transform rules. first one simply generate original edge, and second rule will replace to value with string interpolation.
```
{
    "size": 6,
    "degrees": [
        {
            "from": 101,
            "label": "s2graph_label_test_weak",
            "direction": "out",
            "_degree": 3
        },
        {
            "from": 101,
            "label": "s2graph_label_test_weak",
            "direction": "out",
            "_degree": 3
        }
    ],
    "results": [
        {
            "cacheRemain": -8,
            "from": 101,
            "to": "10",
            "label": "s2graph_label_test_weak",
            "direction": "out",
            "_timestamp": 6,
            "timestamp": 6,
            "score": 1,
            "props": {
                "_timestamp": 6,
                "time": -30,
                "weight": 0,
                "is_hidden": false,
                "is_blocked": false
            }
        },
        {
            "cacheRemain": -8,
            "from": 101,
            "to": "time.-30",
            "label": "s2graph_label_test_weak",
            "direction": "out",
            "_timestamp": 6,
            "timestamp": 6,
            "score": 1,
            "props": {
                "_timestamp": 6,
                "time": -30,
                "weight": 0,
                "is_hidden": false,
                "is_blocked": false
            }
        },
        {
            "cacheRemain": -8,
            "from": 101,
            "to": "10",
            "label": "s2graph_label_test_weak",
            "direction": "out",
            "_timestamp": 5,
            "timestamp": 5,
            "score": 1,
            "props": {
                "_timestamp": 5,
                "time": -10,
                "weight": 0,
                "is_hidden": false,
                "is_blocked": false
            }
        },
        {
            "cacheRemain": -8,
            "from": 101,
            "to": "time.-10",
            "label": "s2graph_label_test_weak",
            "direction": "out",
            "_timestamp": 5,
            "timestamp": 5,
            "score": 1,
            "props": {
                "_timestamp": 5,
                "time": -10,
                "weight": 0,
                "is_hidden": false,
                "is_blocked": false
            }
        },
        {
            "cacheRemain": -8,
            "from": 101,
            "to": "10",
            "label": "s2graph_label_test_weak",
            "direction": "out",
            "_timestamp": 4,
            "timestamp": 4,
            "score": 1,
            "props": {
                "_timestamp": 4,
                "time": 0,
                "weight": 0,
                "is_hidden": false,
                "is_blocked": false
            }
        },
        {
            "cacheRemain": -8,
            "from": 101,
            "to": "time.0",
            "label": "s2graph_label_test_weak",
            "direction": "out",
            "_timestamp": 4,
            "timestamp": 4,
            "score": 1,
            "props": {
                "_timestamp": 4,
                "time": 0,
                "weight": 0,
                "is_hidden": false,
                "is_blocked": false
            }
        }
    ],
    "impressionId": 1972178414
}
```

##### 8. 2 step traverse example #####

chaining multiple step will yield traverse query. following query will find friends of friends.

```javascript
{
    "srcVertices": [{"serviceName": "s2graph", "columnName": "account_id", "id":1}],
    "steps": [
	  {
		  "step": [
			{"label": "friends", "direction": "out", "limit": 100}	  
		  ]
	  }, 
	  {
		  "step": [
			{"label": "friends", "direction": "out", "limit": 10}
		  ]
	  }
    ]
}
'
```
##### 9. 3 step traverse example #####

just like 2 step, add more steps. watch out limit on each step since search space is equal to multiplication of max limits on each step.


##### 10. more examples #####
Example 1. Selecting the first 100 edges of label `graph_test`, which start from the vertex with `account_id=1`, sorted using the default index of `graph_test`.

```javascript
curl -XPOST localhost:9000/graphs/getEdges -H 'Content-Type: Application/json' -d '
{
    "srcVertices": [{"serviceName": "s2graph", "columnName": "account_id", "id":1}],
    "steps": [
      [{"label": "graph_test", "direction": "out", "offset": 0, "limit": 100
      }]
    ]
}
'
```

Example 2. Selecting the 50th ~ 100th edges from the same vertex.

```javascript
curl -XPOST localhost:9000/graphs/getEdges -H 'Content-Type: Application/json' -d '
{
    "srcVertices": [{"serviceName": "s2graph", "columnName": "account_id", "id":1}],
    "steps": [
      [{"label": "graph_test", "direction": "in", "offset": 50, "limit": 50}]
    ]
}
'
```

Example 3. Selecting the 50th ~ 100th edges from the same vertex, now with a time range filter.

```javascript
curl -XPOST localhost:9000/graphs/getEdges -H 'Content-Type: Application/json' -d '
{
    "srcVertices": [{"serviceName": "s2graph", "columnName": "account_id", "id":1}],
    "steps": [
      [{"label": "graph_test", "direction": "in", "offset": 50, "limit": 50, "duration": {"from": 1416214118, "to": 1416214218}]
    ]
}
'
```

Example 4. Selecting 50th ~ 100th edges from the same vertex, sorted using the indexed properties `time` and `weight`, with the same time range filter, and applying weighted sum using `time: 1.5, weight: 10`


```javascript
curl -XPOST localhost:9000/graphs/getEdges -H 'Content-Type: Application/json' -d '
{
    "srcVertices": [{"serviceName": "s2graph", "columnName": "account_id", "id":1}],
    "steps": [
      [{"label": "graph_test", "direction": "in", "offset": 50, "limit": 50, "duration": {"from": 1416214118, "to": 1416214218}, "scoring": {"time": 1.5, "weight": 10}]
    ]
}
'
```

Example 5. Selecting 100 edges representing `friends`, from the vertex with `account_id=1`, and again selecting their 10 friends, therefore selecting at most 1,000 "friends of friends".

```javascript
curl -XPOST localhost:9000/graphs/getEdges -H 'Content-Type: Application/json' -d '
{
    "srcVertices": [{"serviceName": "s2graph", "columnName": "account_id", "id":1}],
    "steps": [
      [{"label": "friends", "direction": "out", "limit": 100}],
      [{"label": "friends", "direction": "out", "limit": 10}]
    ]
}
'
```

Example 6. Selecting 100 edges representing `friends` and their 10 `listened_music` edges, to get "music that my friends have listened to".

```javascript
curl -XPOST localhost:9000/graphs/getEdges -H 'Content-Type: Application/json' -d '
{
    "srcVertices": [{"serviceName": "s2graph", "columnName": "account_id", "id":1}],
    "steps": [
      [{"label": "talk_friend", "direction": "out", "limit": 100}],
      [{"label": "play_music", "direction": "out", "limit": 10}]
    ]
}
'
```

Example 7. my friends who played music id 200
```javascript
curl -XPOST localhost:9000/graphs/getEdges -H 'Content-Type: Application/json' -d '
{
    "srcVertices": [{"serviceName": "s2graph", "columnName": "account_id", "id":1}],
    "steps": [
      [{"label": "talk_friend", "direction": "out", "limit": 100}],
      [{"label": "play_music", "direction": "out", "_to": 200}]
    ]
}
'
```

Example 8. more general way to check list of edges exist.

```javascript
curl -XPOST localhost:9000/graphs/checkEdges -H 'Content-Type: Application/json' -d '
[
	{"label": "talk_friend", "direction": "out", "from": 1, "to": 100}, 
	{"label": "talk_friend", "direction": "out", "from": 1, "to": 101}
]
'
```


#### 2.2. Vertex Queries ####

##### 1. POST /graphs/getVertices #####
Selecting all vertices from column `account_id` of a service `s2graph`.

```javascript
curl -XPOST localhost:9000/graphs/getVertices -H 'Content-Type: Application/json' -d '
[
	{"serviceName": "s2graph", "columnName": "account_id", "ids": [1, 2, 3]},
	{"serviceName": "agit", "columnName": "user_id", "ids": [1, 2, 3]}
]
'
```


 

## 6. Bulk Loading ##

In many cases, the first step to start using s2graph is to migrate a large dataset into s2graph. s2graph provides a bulk loading script for importing the initial dataset. 

To use bulk load, you need running [Spark](https://spark.apache.org/) cluster and **TSV file** with bulk load format.

Note that if you don't need extra properties on vertices(i.e., you only need vertex id), you only need to publish the edges and not the vertices. Publishing edges will effectively create vertices with empty properties.

#### Edge Format

|timestamp | operation | logType |  from | to | label | props |
|:------- | --- |:----: | --- | -----| --- | --- |
|1416236400|insert|edge|56493|26071316|talk_friend_long_term_agg_by_account_id|{"timestamp":1416236400,"score":0}|

#### Vertex Format

|timestamp | operation | logType |  id | serviceName | columnName | props |
|:------- | --- |:----: | --- | -----| --- | --- |
|1416236400|insert|vertex|56493|kakaotalk|account_id|`{"is_active":true, "country_iso": "kr"}`|

### Build ###
to build bulk loader, you need to build loader project. just run following commend.

> `sbt "project loader" "clean" "assembly"

you will see **s2graph-loader-assembly-0.0.4-SNAPSHOT.jar** under loader/target/scala-2.xx/

### Source Data Storage Options

For bulk loading, source data can be either in HDFS or Kafka queue.

#### 1. When the source data is in HDFS. ###
 
 - run subscriber.GraphSubscriber to bulk upload HDFS TSV file into s2graph. 
 - make sure how many edges are parsed/stored by looking at Spark UI.

#### 2. When the source data is in Kafka. ####
 
assumes that data is bulk loading format and constantly comming into Kafka MQ.

 - run subscriber.GraphSubscriberStreaming to extract and load into s2graph from kafka topic.
 - make sure how many edges are parsed/stored by looking at Spark UI.

#### 3. online migration ####
following is the way we do online migration from RDBMS to s2graph. assumes that client send same events that goes to primary storage(RDBMS) and s2graph.

 - mark label as isAsync true. this will queue all events into kafka queue.
 - dump RDBMS and build bulk load file in TSV. 
 - update TSV file with subscriber.GraphSubscriber.
 - mark label as isAsync false. this will stop queuing events into kafka queue and apply changes into s2graph directly. 
 - since s2graph is Idempotent, it is safe to replay queued message while bulk load process. so just use subscriber.GraphSubscriberStreaming to queued events.


## 7. Benchmark ##


### Test data
1. kakao talk full graph(8.8 billion edges)
2. sample 10 million user id that have more than 100 friends.
3. number of region server for HBase = 20

#### 1. friend of friend
**find 50 talk friends then find 20 talk friends**
```
 {
    "srcVertices": [{"serviceName": "kakaotalk", "columnName": "talk_user_id", "id":$id}],
    "steps": [
      [{"label": "talk_friend", "direction": "out", "limit": 50}],
      [{"label": "talk_friend", "direction": "out", "limit": 20}]
    ]
	}
```

total vuser = 980

| number of rest server |  tps | mean test time | 
|:------- | --- |:----: | --- |
| 10 | 5,981.5 | 151.36 ms | 
| 20 | 10,589 | 86.45 ms |
| 30 | 16,295.4 | 56.43 ms | 


#### 2. friends
**find 100 talk friends**
```
 {
    "srcVertices": [{"serviceName": "kakaotalk", "columnName": "talk_user_id", "id":$id}],
    "steps": [
      [{"label": "talk_friend", "direction": "out", "limit": 100}]
    ]
	}
```

total vuser = 2,072

| number of rest server |  tps | mean test time |  
|:------- | --- |:----: | --- |
| 20 | 53,713.4 | 37.31 ms | 



### new benchmark (asynchbase) ###


#### 1. one step query
```
{
    "srcVertices": [
        {
            "serviceName": "kakaotalk",
            "columnName": "talk_user_id",
            "id": %s    
        }
    ],
    "steps": [
      [
        {
          "label": "talk_friend_long_term_agg", 
          "direction": "out", 
          "offset": 0, 
          "limit": %d
        }
      ]
    ]
}
```
| number of rest server |  vuser | offset | first step limit | tps | latency | 
|:------- | --- |:----: | --- | --- | --- | --- |
| 1 | 30 | 0 | 10 | 9790TPS | 3ms | 
| 1 | 30 | 80 | 10 |  9,958.2TPS | 2.91ms |
| 1 | 30 | 0 | 20 |  7,418.1TPS | 3.92ms | 
| 1 | 30 | 0 | 40 | 5,118.5TPS | 5.72ms | 
| 1 | 30 | 0 | 60 | 3,966.9TPS | 7.38ms | 
| 1 | 30 | 0 | 80 | 3,408.4TPS | 8.58ms | 
| 1 | 30 | 0 | 100 | 3,048.1TPS | 9.76ms | 
| 2 | 60 | 0 | 100 | 5,869.4TPS | 10.04ms | 
| 4 | 120 | 0 | 100 | 11,473.1TPS | 10.27ms | 

#### 2. two step query
```
{
    "srcVertices": [
        {
            "serviceName": "kakaotalk",
            "columnName": "talk_user_id",
            "id": %s    
        }
    ],
    "steps": [
      [
        {
          "label": "talk_friend_long_term_agg", 
          "direction": "out", 
          "offset": 0, 
          "limit": %d
        }
      ], 
      [
        {
          "label": "talk_friend_long_term_agg", 
          "direction": "out", 
          "offset": 0, 
          "limit": %d
        }
      ]
    ]
}
```
| number of rest server |  vuser | first step limit | second step limit | tps | latency |
|:------- | --- |:----: | --- | --- | --- | --- |  
| 1 | 30 | 10 | 10 | 2,008.2TPS | 14.7ms  | 
| 1 | 30 | 10 | 20 | 1,221.3TPS | 24.13ms | 
| 1 | 30 | 10 | 40 | 678TPS | 43.92ms |
| 1 | 30 | 10 | 60 | 488.2TPS | 60.72ms | 
| 1 | 30 | 10 | 80 | 360.2TPS | 82.55ms | 
| 1 | 30 | 10 | 100 | 312.1TPS | 94.7ms | 
| 1 | 20 | 10 | 100 | 297TPS | 66.73ms |
| 1 | 10 | 10 | 100 | 302TPS | 32.86ms | 
| 1 | 30 | 20 | 10 | 1163.3TPS | 25.5ms | 
| 1 | 30 | 20 | 20 | 645.9TPS | 45.79ms | 
| 1 | 30 | 40 | 10 | 618.4TPS | 47.96ms | 
| 1 | 30 | 60 | 10 | 448.9TPS | 66.16ms | 
| 1 | 30 | 80 | 10 | 339.3TPS | 87.82ms | 
| 1 | 30 | 100 | 10 | 272.5TPS | 108.65ms | 
| 1 | 20 | 100 | 10  | 288.5TPS | 68.34ms | 
| 1 | 10 | 100 | 10 | 261.4TPS | 37.49ms | 
| 2 | 60 | 100 | 10 | 412.9TPS | 143.83ms | 
| 4 | 120 | 100 | 10 | 791.7TPS | 150.06ms | 

#### 3. three step query
```
{
    "srcVertices": [
        {
            "serviceName": "kakaotalk",
            "columnName": "talk_user_id",
            "id": %s    
        }
    ],
    "steps": [
      [
        {
          "label": "talk_friend_long_term_agg", 
          "direction": "out", 
          "offset": 0, 
          "limit": %d
        }
      ], 
      [
        {
          "label": "talk_friend_long_term_agg", 
          "direction": "out", 
          "offset": 0, 
          "limit": %d
        }
      ],
      [
        {
          "label": "talk_friend_long_term_agg", 
          "direction": "out", 
          "offset": 0, 
          "limit": %d
        }
      ]
    ]
}
```
| number of rest server |  vuser | first step limit | second step limit | third step limit | tps | latency |
|:------- | --- |:----: | --- | --- | --- | --- | --- |  
| 1 | 30 | 10 | 10 | 10 | 250.2TPS | 118.86ms | 
| 1 | 30 | 10 | 10 | 20 | 90.4TPS | 329.46ms | 
| 1 | 20 | 10 | 10 | 20 | 83.2TPS | 238.42ms | 
| 1 | 10 | 10 | 10 | 20 | 82.6TPS | 120.16ms | 


## 8. Resources ##
* [hbaseconf](http://hbasecon.com/agenda): presentation is not published yet, but you can find our [keynote](https://www.dropbox.com/s/scn2dtpflj931mw/hbasecon_s2graph_final.key?dl=0)
* mailing list: use [google group](https://groups.google.com/forum/#!forum/s2graph) or fire issues on this repo.
* contact: shom83@gmail.com




[![Analytics](https://ga-beacon.appspot.com/UA-62888350-1/s2graph/readme.md)](https://github.com/daumkakao/s2graph)
