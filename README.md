
**S2Graph**
===================

**S2Graph** is a **graph database** designed to handle transactional graph processing at scale. Its REST API allows you to store, manage and query relational information using **edge** and **vertex** representations in a **fully asynchronous** and **non-blocking** manner. This document covers some basic concepts and terms of S2Graph as well as help you get a feel for the S2Graph API.


Table of Content
-------------
<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](http://doctoc.herokuapp.com/)*

- [S2Graph](#)
	- [Table of Content](#)
	- [Getting Started](#)
	- [The Data Model](#)
	- [REST API Glossary](#)
	- [1. Creating a Service - POST /graphs/createService](#)
		- [1.1 Service Fields](#)
		- [1.2 Basic Service Operations](#)
	- [2. Creating a Label - POST /graphs/createLabel](#)
		- [2.1 Label Fields](#)
		- [2.2 Basic Label Operations](#)
		- [2.3 Adding Extra Properties to Labels](#)
		- [2.4 Consistency Level](#)
		- [2.4 Adding Extra Indices (Optional) - POST /graphs/addIndex](#)
	- [3. Creating a Service Column (Optional) - POST /graphs/createServiceColumn](#)
		- [3.1 Service Column Fields](#)
		- [3.2 Basic Service Column Operations](#)
	- [4. Managing Edges](#)
		- [4.1 Edge Fields](#)
		- [4.2 Basic Edge Operations](#)
			- [Strong Consistency](#)
				- [1. Insert - POST /graphs/edges/insert](#)
				- [2. Delete -POST /graphs/edges/delete](#)
				- [3. Update -POST /graphs/edges/update](#)
				- [4. Increment -POST /graphs/edges/increment](#)
				- [5. Delete All - POST /graphs/edges/deleteAll](#)
			- [Weak Consistency](#)
				- [1. Insert - POST /graphs/edges/insert](#)
				- [2. Delete -POST /graphs/edges/delete](#)
				- [3. Update -POST /graphs/edges/update](#)
				- [4. Increment -POST /graphs/edges/increment](#)
				- [5. Delete All - POST /graphs/edges/deleteAll](#)
	- [5. Managing Vertices (Optional)](#)
		- [5.1 Vertex Fields](#)
		- [5.2 Basic Vertex Operations](#)
			- [1. Insert - POST /graphs/vertices/insert/:serviceName/:columnName](#)
			- [2. Delete - POST /graphs/vertices/delete/:serviceName/:columnName](#)
			- [3. Delete All - POST /graphs/vertices/deleteAll/:serviceName/:columnName](#)
			- [4. Update - POST /graphs/vertices/update/:serviceName/:columnName](#)
			- [5. Increment](#)
	- [6. Querying](#)
		- [6.1. Query Fields](#)
		- [6.2. Query API](#)
			- [6.2.1. Edge Queries](#)
				- [1. POST /graphs/getEdges](#)
				- [2. POST /graphs/checkEdges](#)
			- [6.2.2 getEdges Examples](#)
				- [1. Duplicate Policy](#)
				- [2. Select Option Example](#)
				- [3. groupBy Option Example](#)
				- [4. filterOut option example](#)
				- [5. nextStepLimit Example](#)
				- [6. nextStepThreshold Example](#)
				- [7. sample Example](#)
				- [8. transform Example](#)
				- [9. Two-Step Traversal Example](#)
				- [10. Three-Step Traversal Example](#)
				- [11. More examples](#)
			- [6.2.3. Vertex Queries](#)
				- [1. POST /graphs/getVertices](#)
	- [7. Bulk Loading](#)
			- [Edge Format](#)
			- [Vertex Format](#)
		- [Build](#)
		- [Source Data Storage Options](#)
			- [1. For source data in HDFS](#)
			- [2. For source data is in Kafka](#)
			- [3. online migration](#)
	- [8. Benchmark](#)
		- [Test data](#)
			- [1. One-Step Query](#)
			- [2. two step query](#)
			- [3. three step query](#)
	- [8. Resources](#)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->



Getting Started
-------------

S2Graph consists of a number of modules.

1. **S2Core** is the core library for common classes to store and retrieve data as edges and vertices.
2. **Root Project** is the Play-Framework-based REST API.
3. **Spark** contains spark-related common classes.
4. **Loader** has spark applications for data migration purposes. Load data to the HBase back-end as graph representations (using the S2Core library) by either consuming events from Kafka or copying straight from HDFS.
5. **Asynchbase** is a fork of https://github.com/OpenTSDB/asynchbase. We added a few functionalities to GetRequest which are yet to be merged to the original project. Here are some of the tweeks listed:
	- rpcTimeout
	- setFilter
	- column pagination
	- retryAttempCount
	- timestamp filtering


----------

There are some prerequisites for running S2Graph:

1. An [SBT](http://www.scala-sbt.org/) installation
  - `> brew install sbt` if you are on a Mac. (Otherwise, checkout the [SBT document](http://www.scala-sbt.org/0.13/tutorial/Manual-Installation.html).)
2. An [Apache HBase](http://hbase.apache.org/) installation
	- `> brew install HBase` if you are on a Mac. (Otherwise, checkout the [HBase document](http://hbase.apache.org/book.html#quickstart).)
  - Run `> start-hbase.sh`.
	- Please note that we currently support latest stable version of **Apache HBase 1.0.1 with Apache Hadoop version 2.7.0**. If you are using CDH, checkout **feature/cdh5.3.0**. @@@We are working on providing a profile on HBase/Hadoop version soon.
3. S2Graph currently supports MySQL for metadata storage.
  - `> brew install mysql` if you are on a Mac. (Otherwise, checkout the [MySQL document](https://dev.mysql.com/doc/refman/5.7/en/general-installation-issues.html).)
  - Run `> mysql.server start`.
  - Connect to MySQL server; `> mysql -uroot`.
  - Create database 'graph_dev'; `mysql> create database graph_dev;`.
  - Configure access rights; `mysql> grant all privileges on graph_dev.* to 'graph'​@localhost identified by 'graph'`.

----------

With the prerequisites are setup correctly, checkout the project:
```
> git clone https://github.com/daumkakao/s2graph.git
> cd s2graph
```
Create necessary MySQL tables by running the provided SQL file:
```
> mysql -ugraph -p < ./s2core/migrate/mysql/schema.sql
```
Now, go ahead and build the project:
```
> sbt compile
```
You are ready to run S2Graph!

```
> sbt run
```

We also provide a simple script under script/test.sh so that you can see if everything is setup correctly.

```
> sh script/test.sh
```

Finally, join the [mailing list](https://groups.google.com/forum/#!forum/s2graph)!


The Data Model
--------------

There are four important concepts that form the data model used throughout S2Graph; services, columns, labels and properties.

**Services**, the top level abstraction, act like databases in traditional RDBMS in which all data are contained. At Kakao, a service usually represents one of the company's actual services and is named accordingly, e.g. `"KakaoTalk"`, `"KakaoStory"`.

**Columns** are names for vertices and a service can have multiple columns. For example, a service `"KakaoMusic"` can have columns `"user_id"` and `"track_id"`.

**Labels**, represent relations between two columns. Think of it as a name for edges. The two columns can be the same (e.g. for a label representing friendships in an SNS, the two column will both be `"user_id"` of the service). Labels can connect columns from two different services. For example, one can create a label that stores all events where KakaoStory posts are shared to KakaoTalk.

**Properties**, are metadata linked to vertices or edges that can be queried upon later. For vertices representing KakaoTalk users, `date_of_birth` is a possible property, and for edges representing similar KakaoMusic songs their `similarity_distance` can be a property.

Using these abstractions, a unique vertex can be identified with its `(service, column, vertex id)`, and a unique edge can be identified with its `(service, label, source vertex id, target vertex id)`. Additional information on edges and vertices are stored within their properties.


REST API Glossary
-----------------

The following is a non-exhaustive list of commonly used S2Graph APIs and their examples. The full list of the latest REST API can be found in [the routes file](conf/routes).

## 1. Creating a Service - `POST /graphs/createService`  ##
Service is the top level abstraction in S2Graph which could be considered as a database in MySQL.

### 1.1 Service Fields
In order to create a Service, the following fields should be specified in the request.

|Field Name |  Definition | Data Type |  Example | Note |
|:------- | --- |:----: | --- | :-----|
| **serviceName** | User defined namespace. | String | "talk_friendship"| Required. |
| cluster | Zookeeper quorum address for your cluster.| String | "abc.com:2181, abd.com:2181" | Optional. <br>By default, S2Graph looks for "hbase.zookeeper.quorum" in your application.conf. If "hbase.zookeeper.quorum" is undefined, this value is set as "localhost". |
| hTableName | HBase table name.|String| "test"| Optional. <br> Default is {serviceName}-{phase}. <br> Phase is usually one of dev, alpha, real, or sandbox. |
| hTableTTL | Global time to live setting for data. | Integer | 86000 | Optional. Default is NULL which means that the data lives forever.
| preSplitSize | Factor for the HBase table pre-split size. **Number of pre-splits = preSplitSize x number of region servers**.| Integer|1|Optional. <br> If you set preSplitSize to 2 and have N region servers in your HBase, S2Graph will pre-split your table into [2 x N] parts. Default is 1.|

### 1.2 Basic Service Operations
You can create a service using the following API:

```
curl -XPOST localhost:9000/graphs/createService -H 'Content-Type: Application/json' -d '
{"serviceName": "s2graph", "cluster": "address for zookeeper", "hTableName": "hbase table name", "hTableTTL": 86000, "preSplitSize": 2}
'
```
> It is recommended that you stick to the default values if you're unsure of what some of the parameters do. Feel free to ask through  

You can also look up all labels that belongs to a service.

```
curl -XGET localhost:9000/graphs/getLabels/:serviceName
```



## 2. Creating a Label - `POST /graphs/createLabel` ##


----------

A Label represents a relation between two columns. Labels are to S2Graph what tables are to RDBMS since they contain the schema information, i.e. descriptive information of the data being managed or indices used for efficient retrieval. In most scenarios, defining an edge schema (in other words, label) requires a little more care compared to a vertex schema (which is pretty straightforward). First, think about the kind of queries you will be using, then, model user actions or relations into **edges** and design a label accordingly.

### 2.1 Label Fields
A Label creation request includes the following information.

|Field Name        | Definition                                                                                                                            | Data Type             | Example                   | Remarks |
|:---------------- | ------------------------------------------------------------------------------------------------------------------------------------- |:--------------------: | ------------------------- | :-----|
| **label**        | Name of the relation. It's better to be specific.                                                                                     | String                | "talk_friendship"         | Required. |
| srcServiceName   | Source column's service.                                                                                                              | String                | "kakaotalk"               | Required. |
| srcColumnName    | Source column's name.                                                                                                                 | String                | "user_id"                 | Required. |
| srcColumnType    | Source column's data type.                                                                                                            | Long/ Integer/ String | "string"                  | Required. |
| tgtServiceName   | Target column's service.                                                                                                              | String                | "kakaotalk"/ "kakaomusic" | Optional. srcServiceName is used by default. |
| tgtColumnName    | Target column's name.                                                                                                                 | String                | "item_id"                 | Required.|
| tgtColumnType    | Target column's data type.                                                                                                            | Long/ Integer/ String | "long"                    | Required. |
| isDirected       | Wether the label is directed or undirected.                                                                                           | True/ False           | "true"/ "false"           | Optional. Default  is "true". |
| **serviceName**  | Which service the label belongs to.                                                                                               | String                | "kakaotalk"               | Optional. tgtServiceName is used by default. |
| hTableName       | A dedicated HBase table to your Label for special usecases (such as batch inserts).                                                   | String                | "s2graph-batch"           | Optional. Service hTableName is used by default. |
| hTableTTL        | Data time to live setting.                                                                                                            | Integer               | 86000                     | Optional. Service hTableTTL is used by default. |
| consistencyLevel | If set to "strong", only one edge is alowed between a pair of source/ target vertices. Set to "weak", and multiple-edge is supported. | String                | "strong"/ "weak"          | Optional. Default is "weak". |
| **indices**      | Please refer to below. |
| **props**        | Please refer to below. |

A couple of key elements of a Label are its Properties (**props**) and **indices**.

Supplementary information of a Vertex or Edge can be stored as props. A single property can be defined in a simple key-value JSON as follows:
```
{
    "name": "name of property",
    "dataType": "data type of property value",
    "defaultValue": "default value in string"
}
```

In a scenario where user - video playback history is stored in a Label, a typical example for props would look like this:
```
[
    {"name": "play_count", "defaultValue": 0, "dataType": "integer"},
    {"name": "is_hidden","defaultValue": false,"dataType": "boolean"},
    {"name": "category","defaultValue": "jazz","dataType": "string"},
    {"name": "score","defaultValue": 0,"dataType": "float"}
]
```

Props can have data types of **numeric** (byte/ short/ integer/ float/ double), **boolean** or **string**.

In order to achieve efficient data retrieval, a Label can be indexed using the "indices" option.

Default value for indices is "_timestamp", a hidden label property. (All labels have _timestamp in their props under the hood.)

The first index in indices array will be the primary index (Think of `PRIMARY INDEX idx_xxx`(`p1, p2`) in MySQL).

S2Graph will automatically store edges according to the primary index.

Trailing indices are used for multiple ordering on edges. (Think of `ALTER TABLE ADD INDEX idx_xxx`(`p2, p1`) in MySQL).

**props** define meta datas that will not be affect the order of edges.

Please avoid using S2Graph-reserved property names:

>1. **_timestamp** is reserved for system wise timestamp. this can be interpreted as last_modified_at
>2. **_from** is reserved for label`s start vertex.
>3. **_to** is reserved for

### 2.2 Basic Label Operations

Here is an sample request that creates a label `user_article_liked` between column `user_id` of service `s2graph` and column `article_id` of service `s2graph_news`. Note that the default indexed property `_timestamp` will be created since the `indexedProps` field is empty.

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

The created label "user_article_liked" will manage edges in a timestamp-descending order (which seems to be the common requirement for most services).


Here is another example that creates a label `friends`, which represents the friend relation between users in service `s2graph`. This time, edges are managed by both affinity_score and _timestamp. Friends with higher affinity_scores come first and if affinity_score is a tie, recently added friends comes first.

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

S2Graph supports **multiple indices** on a label which means you can add separate ordering options for edges.

```
# with existing props
curl -XPOST localhost:9000/graphs/addIndex -H 'Content-Type: Application/json' -d '
{
    "label": "friends",
    "indices": [
        {"name": "idx_3rd", "propNames": ["is_blocked", "_timestamp"]}
    ]
}
'
```

In order to get general information on a label, make a GET request to `/graphs/getLabel/{label name}`:

```
curl -XGET localhost:9000/graphs/getLabel/friends
```

Delete a label with a PUT request to `/graphs/deleteLabel/{label name}`.

```
curl -XPUT localhost:9000/graphs/deleteLabel/friends
```

Label updates are not supported (except when you are adding an index). Instead, you can delete the label and re-create it.



### 2.3 Adding Extra Properties to Labels

To add a new property, use `/graphs/addProp/{label name}`:

```
curl -XPOST localhost:9000/graphs/addProp/friend -H 'Content-Type: Application/json' -d '
{"name": "is_blocked", "defaultValue": false, "dataType": "boolean"}
'
```

### 2.4 Consistency Level
Simply put, the consistency level of your label will determine how the edges are stored at storage level.

First, note that S2Graph identifies a unique edge by combining its from, label, to values as a key.

Now, let's consider inserting the following two edges that have same keys (1, graph_test, 101) and different timestamps (1418950524721 and 1418950524723).
> ```
> 1418950524721	insert  e 1 101	graph_test	{"weight": 10} = (1, graph_test, 101)
> 1418950524723	insert	e	1	101	graph_test	{"weight": 20} = (1, graph_test, 101)
> ```
Each consistency levels handle the case differently.


**1. strong**
> The strong option makes sure that there is **only one edge record stored in the HBase table** for edge key (1, graph_test, 101).
> With strong consistency level, the later insertion will overwrite the previous one.

**2. weak**
> The weak option will allow **two different edges stored in the table** with different timestamps and weight values.

For a better understanding, let's simplify the notation for an edge that connects two vertices u - v at time t as u -> (t, v), and assume that we are inserting these four edges into two different labels with each consistency configuration (both indexed by timestamp only).

```
u1 -> (t1, v1)
u1 -> (t2, v2)
u1 -> (t3, v2)
u1 -> (t4, v1)
```

With a strong consistencyLevel, your Label contents will be:
```
u1 -> (t4, v1)
u1 -> (t3, v2)
```
Note that edges with same vertices and earlier timestamp (u1 -> (t1, v1) and u1 -> (t2, v2)) were overwritten and do not exist.

On the other hand, with consistencyLevel weak.
```
u1 -> (t1, v1)
u1 -> (t2, v2)
u1 -> (t3, v2)
u1 -> (t4, v1)
```


> It is recommended to set consistencyLevel to **weak** unless you are expecting **concurrent updates on same edge**.

In real world systems, it is not guaranteed that operation requests arrive at S2Graph in the order of their timestamp. Depending on the environment (network conditions, client making asynchronous calls, use of a message que, and so on) request that were made earlier can arrive later. Consistency level also determines how S2Graph handles these cases.

Strong consistencyLevel promises a final result consistent to the timestamp.

For example, consider a set of operation requests on edge (1, graph_test, 101) were made in the following order;
```
1418950524721	insert	e	1	101	graph_test	{"is_blocked": false}
1418950524722	delete	e	1	101	graph_test
1418950524723	insert	e	1	101	graph_test	{"is_hidden": false, "weight": 10}
1418950524724	update	e	1	101	graph_test	{"time": 1, "weight": -10}
1418950524726	update	e	1	101	graph_test	{"is_blocked": true}
```

and actually arrived in a shuffled order due to complications:
```
1418950524726	update	e	1	101	graph_test	{"is_blocked": true}
1418950524723	insert	e	1	101	graph_test	{"is_hidden": false, "weight": 10}
1418950524722	delete	e	1	101	graph_test
1418950524721	insert	e	1	101	graph_test	{"is_blocked": false}
1418950524724	update	e	1	101	graph_test	{"time": 1, "weight": -10}
```
Strong consistency still makes sure that you get the same eventual state on (1, graph_test, 101).


Here is pseudocode of what S2Graph does to provide a strong consistency level.
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


>**Limitations**
>Since S2Graph makes asynchronous writes to HBase via Asynchbase, there is no consistency guaranteed on same edge within its flushInterval (1 second).



### 2.4 Adding Extra Indices (Optional) - `POST /graphs/addIndex` ###


----------


A label can have multiple properties set as indexes. When edges are queried, the ordering will determined according to indexes, therefore, deciding which edges will be included in the **top-K** results.

> Edge retrieval queries in S2Graph by default returns **top-K** edges. Clients must issue another query to fetch the next K edges, i.e., **top-K ~ 2 x top-K**.

Edges sorted according to the indices in order to limit the number of edges being fetched by a query. If no ordering property is given, S2Graph will use the **timestamp** as an index, thus resulting in the most recent data.

> It would be extremely difficult to fetch millions of edges and sort them at request time and return a top-K in a reasonable amount of time. Instead, S2Graph uses vertex-centric indexes to avoid this.
>
> **Using a vertex-centric index, having millions of edges is fine as long as size K of the top-K values is reasonable (under 1K)**
> **Note that indexes must be created prior to inserting any data on the label** (which is the same case with the conventional RDBMS).

New indexes can be dynamically added, but will not be applied to pre-existing data (support for this is planned for future versions). **Currently, a label can have up to eight indices.**

The following is an example of adding index `play_count` to a label `graph_test`.

```
// add prop first
curl -XPOST localhost:9000/graphs/addProp/graph_test -H 'Content-Type: Application/json' -d '
  { "name": "play_count", "defaultValue": 0, "dataType": "integer" }
'

// then add index
curl -XPOST localhost:9000/graphs/addIndex -H 'Content-Type: Application/json' -d '
{
    "label": "graph_test",
    "indices": [
        { name: "idx_play_count", propNames: ["play-count"] }
    ]
}
'
```

## 3. Creating a Service Column (Optional) - `POST /graphs/createServiceColumn`
----------
If your use case requires props assigned to vertices instead of edges, what you need is a Service Column.

>**Remark: If it is only the vertex id that you need and not additional props, there's no need to create a Service Column explicitly. At label creation, by default, S2Graph creates column space with empty properties according to the label schema.**

### 3.1 Service Column Fields

| Field Name |  Definition | Data Type |  Example | Remarks |
|:------- | --- |:----: | --- | :-----|
| serviceName | Which service the Service Column belongs to. | String | "kakaotalk" | Required. |
| columnName | Service Column`s name. | String |  "talk_user_id" | Required. |
| props | Optional properties of Service Column. | JSON (array dictionaries) | Please refer to the examples. | Optional.|

### 3.2 Basic Service Column Operations
Here are some sample requests for Service Column creation as well as vertex insertion and selection.
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

General information on a vertex schema can be retrieved with `/graphs/getServiceColumn/{service name}/{column name}`.

```
curl -XGET localhost:9000/graphs/getServiceColumn/s2graph/user_id
```
This will give all properties on serviceName `s2graph` and columnName `user_id` serviceColumn.

Properties can be added to a Service Column with `/graphs/addServiceColumnProps/{service name}/{column name}`.

```
curl -XPOST localhost:9000/graphs/addServiceColumnProps/s2graph/user_id -H 'Content-Type: Application/json' -d '
[
	{"name": "home_address", "defaultValue": "korea", "dataType": "string"}
]
'
```

Vertices can be inserted to a Service Column using `/graphs/vertices/insert/{service name}/{column name}`.
```
curl -XPOST localhost:9000/graphs/vertices/insert/s2graph/user_id -H 'Content-Type: Application/json' -d '
[
  {"id":1,"props":{"is_active":true}, "timestamp":1417616431},
  {"id":2,"props":{},"timestamp":1417616431}
]
'
```

Finally, query your vertex via `/graphs/getVertices`.
```
curl -XPOST localhost:9000/graphs/getVertices -H 'Content-Type: Application/json' -d '
[
	{"serviceName": "s2graph", "columnName": "user_id", "ids": [1, 2, 3]}
]
'
```

##4. Managing Edges ##
An **Edge** represents a relation between two vertices, with properties according to the schema defined in its label.

### 4.1 Edge Fields
The following fields need to be specified when inserting an edge, and are returned when queried on edges.

| Field Name |  Definition | Data Type | Example | Remarks |
|:------- | --- |:----: | --- | :-----|
| **timestamp** | Issue time of request. | Long | 1430116731156 | Required. Unix Epoch time in **milliseconds**. S2Graph TTL and timestamp unit is milliseconds.  |
| operation | One of insert, delete, update, or increment. | String |  "i", "insert" | Required only for bulk operations. Aliases are also available: i (insert), d (delete), u (update), in (increment). Default is insert.|
| from |  Id of source vertex. |  Long/ String  |1| Required. Use long if possible. **Maximum string byte-size is 249** |
| to | Id of target vertex. |  Long/ String |101| Required. Use long if possible. **Maximum string byte-size is 249** |
| label | Label name  | String |"graph_test"| Required. |
| direction | Direction of the edge. Should be one of **out/ in/ undirected** | String | "out" | Required. Alias are also available: o (out), i (in), u (undirected)|
| props | Additional properties of the edge. | JSON (dictionary) | {"timestamp": 1417616431, "affinity_score":10, "is_hidden": false, "is_valid": true}| Required. **If in indexed properties isn't given, default values will be added.** |


### 4.2 Basic Edge Operations

In S2Graph, an Edge supports five different operations.

1. insert: Create new edge.
2. delete: Delete existing edge.
3. update: Update existing edge`s state.
4. increment: Increment existing edge`s state.
5. deleteAll: Delete all adjacent edges from certain source vertex. (Available for strong consistency only.)

Edge operations work differently depending on the target label`s consistency level.

For a better understanding, please take a look at the following test cases.

Create 2 different labels, one of each consistencyLevels.

1. s2graph_label_test (strong)
2. s2graph_label_test_weak (weak)


Then insert a same set of edges to each labels and query them as follows.

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

Note that only one edge exist between (101, 10, s2graph_label_test, out).
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

This time there are **three edges** between (101, 10, s2graph_label_test_weak, out).
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



#### Strong Consistency ####
##### 1. Insert - `POST /graphs/edges/insert` #####

A unique edge is identified by a combination of (from, to, label, direction).
For insert operations, S2Graph first checks if an edge with same (from, to, label, direction) information exists. If there is an existing edge, then insert will work as **update**. See above example.

##### 2. Delete -`POST /graphs/edges/delete` #####
For edge deletion, again, S2Graph looks for a unique edge with (from, to, label, direction).
However, this time it checks the timestamp of the delete request and the existing edge. The timestamp on the delete request **must be larger than that on the existing edge** or else the request will be ignored. If everything is well, the edge will be deleted. Also note that no props information is necessary for a delete request on a strongly consistent label since there will be only one edge with edge`s unique id(from, to, label, direction).

```
curl -XPOST localhost:9000/graphs/edges/delete -H 'Content-Type: Application/json' -d '
[
	{"timestamp": 10, "from": 101, "to": 10, "label": "s2graph_label_test"}
]
'
```
##### 3. Update -`POST /graphs/edges/update` #####
What an update operation does to a strongly consistent label is identical to an insert.
```
curl -XPOST localhost:9000/graphs/edges/update -H 'Content-Type: Application/json' -d '
[
	{"timestamp": 10, "from": 101, "to": 10, "label": "s2graph_label_test", "props": {"time": 100, "weight": -10}}
]
'
```
##### 4. Increment -`POST /graphs/edges/increment` #####
Works like update, other than it returns the incremented value and not the old value.
```
curl -XPOST localhost:9000/graphs/edges/increment -H 'Content-Type: Application/json' -d '
[
	{"timestamp": 10, "from": 101, "to": 10, "label": "s2graph_label_test", "props": {"time": 100, "weight": -10}}
]
'
```

##### 5. Delete All - `POST /graphs/edges/deleteAll` #####
Delete all adjacent edges to the source vertex.
**Please note that edges with both in and out directions will be deleted.**

```
curl -XPOST localhost:9000/graphs/edges/deleteAll -H 'Content-Type: Application/json' -d '
[
  {"ids" : [101], "label":"s2graph_label_test", "direction": "out", "timestamp":1417616441000}
]
'
```

#### Weak Consistency ####
##### 1. Insert - `POST /graphs/edges/insert` #####
S2Graph **does not** look for a unique edge defined by (from, to, label, direction).
It simply stores a new edge according to the request. No read, no consistency check. Note that this difference allows multiple edges with same (from, to, label, direction) id.

##### 2. Delete -`POST /graphs/edges/delete` #####
For deletion on weakly consistent edges, first, S2Graph fetches existing edges from storage.
Then, on each resulting edges, fires the actual delete operations.
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
##### 3. Update -`POST /graphs/edges/update` #####
Like insert, S2Graph **does not** check for uniqueness. Update requires a pre-fetch of existing edges, similar to delete. Props of the resulting edges will be updated.

##### 4. Increment -`POST /graphs/edges/increment` #####
For increment, S2Graph also **does not** check for uniqueness. Update requires a pre-fetch of existing edges, similar to delete. Props of the resulting edges will be incremented.

##### 5. Delete All - `POST /graphs/edges/deleteAll` #####
Identical to strong consistency.
```
curl -XPOST localhost:9000/graphs/edges/deleteAll -H 'Content-Type: Application/json' -d '
[
  {"ids" : [101], "label":"s2graph_label_test", "direction": "out", "timestamp":1417616441}
]
'
```

## 5. Managing Vertices (Optional)

Vertices are the two end points of an edge, and logically stored in columns of a service. If your use case requires storing metadata corresponding to vertices rather than edges, there are operations available on vertices as well.

### 5.1 Vertex Fields
Unlike edges and their labels, properties of a vertex are not indexed nor require a predefined schema. The following fields are used when operating on vertices.

| Field Name |  Definition | Data Type | Example |  Remarks |
|:------- | --- |:----: | --- | :-----|
| timestamp |  | Long |  1417616431 | Required. Unix Epoch time in **milliseconds**. |
| operation | One of insert, delete, update, increment | String | "i", "insert" | Required only for bulk operations. Alias are also available: i (insert), d (delete), u (update), in (increment). Default is insert.|
| **serviceName** | Corresponding service name | String | "kakaotalk"/"kakaogroup"| Required. |
| **columnName** | Corresponding column name |  String  |"user_id"| Required. |
| id     | Unique identifier of vertex |  Long/ String |101| Required. Use Long if possible.|
| **props** | Additional properties of vertex. | JSON (dictionary) | {"is_active_user": true, "age":10, "gender": "F", "country_iso": "kr"}| Required.  |



### 5.2 Basic Vertex Operations

#### 1. Insert - `POST /graphs/vertices/insert/:serviceName/:columnName` ####

```
curl -XPOST localhost:9000/graphs/vertices/insert/s2graph/account_id -H 'Content-Type: Application/json' -d '
[
  {"id":1,"props":{"is_active":true, "talk_user_id":10},"timestamp":1417616431000},
  {"id":2,"props":{"is_active":true, "talk_user_id":12},"timestamp":1417616431000},
  {"id":3,"props":{"is_active":false, "talk_user_id":13},"timestamp":1417616431000},
  {"id":4,"props":{"is_active":true, "talk_user_id":14},"timestamp":1417616431000},
  {"id":5,"props":{"is_active":true, "talk_user_id":15},"timestamp":1417616431000}
]
'
```

#### 2. Delete - `POST /graphs/vertices/delete/:serviceName/:columnName` ####

This operation will delete only the vertex data of a specified column and will **not** delete any edges connected to those vertices.

**Important notes**
>**This means that edges returned by a query can contain deleted vertices. Clients are responsible for checking validity of the vertices.**

#### 3. Delete All - `POST /graphs/vertices/deleteAll/:serviceName/:columnName` ####

This operation will delete all vertices and connected edges in a given column.

```
curl -XPOST localhost:9000/graphs/vertices/deleteAll/s2graph/account_id -H 'Content-Type: Application/json' -d '
[{"id": 1, "timestamp": 193829198000}]
'
```

This is a **very expensive** operation. If you're interested in what goes on under the hood, please refer to the following pseudocode:

```
vertices = vertex list to delete
for vertex in vertices
	labels = fetch all labels that this vertex is included.
	for label in labels
		for index in label.indices
			edges = G.read with limit 50K
			for edge in edges
				edge.delete
```

The total complexity is O(L * L.I) reads + O(L * L.I * 50K) writes, worst case. **If the vertex you're trying to delete has more than 50K edges, the deletion will not be consistent.**



#### 4. Update - `POST /graphs/vertices/update/:serviceName/:columnName` ####

Same parameters as the insert operation.

#### 5. Increment ####

Not yet implemented; stay tuned.


## 6. Querying
Once you have your graph data uploaded to S2Graph, you can traverse your graph using our query APIs.


### 6.1. Query Fields ###

A typical query contains the source vertex as a starting point, a list of labels to traverse, and optional filters or weights for unwanted results and sorting respectively. Query requests are structured as follows:

| Field Name |  Definition | Data Type | Example |  Remarks |
|:------- | --- |:----: | --- | :-----|
| srcVertices | Starting point(s) of the traverse. | JSON (array of vertex representations that includes "serviceName", "columnName", and "id" fields.) | `[{"serviceName": "kakao", "columnName": "account_id", "id":1}]` | Required. |
|**steps**| A list of labels to traverse. | JSON (array of a step representation) |  ```[[{"label": "graph_test", "direction": "out", "limit": 100, "scoring":{"time": 0, "weight": 1}}]] ``` |Explained below.|
|removeCycle| When traversing to next step, disregard vertices that are already visited.| Boolean | "true"/ "false" | "Already visited" vertices are decided by both label and vertex. If a two-step query on a same label "friends" contains any source vertices, removeCycle option will decide whether or not these vertices will be included in the response. Default is "true". |
|select| Edge data fields to include in the query result. | JSON (array of strings) | ["label", "to", "from"] | |
|groupBy | S2Graph will group results by this field. | JSON (array of strings) | ["to"] | |
|filterOut | Give a nested query in this field, and it will run concurrently. The result will be a act as a blacklist and filtered out from the result of the outer query.| JSON (query format) | ||


**step**: Each step tells S2Graph which labels to traverse in the graph and how to traverse them. Labels in the very first step should be directly connected to the source vertices, labels in the second step should be directly connected to the result vertices of the first step traversal, and so on. A step is specified with a list of **query params** which we will cover in detail below.

**step param**:

| Field Name |  Definition | Data Type |  Example | Remarks |
|:------- | --- |:----: | --- | :-----|
| weights | Weight factors for edge scores in a step. These weights will be multiplied to scores of the corresponding edges, thereby boosting certain labels when sorting the end result. | JSON (dictionary) | {"graph_test": 0.3, "graph_test2": 0.2} | Optional. |
| nextStepThreshold | Threshold of the steps' edge scores. Only the edges with scores above nextStepThreshold will continue to the next step | Double |  | Optional.|
| nextStepLimit | Number of resulting edges from the current step that will continue to the next step. | Double | | Optional.|
| sample | You can randomly sample N edges from the corresponding step result. | Integer |5 | Optional.|

**query param**:

| Field Name |  Definition | Data Type | Example |  Remarks |
|:------- | --- |:----: | --- | :-----|
| label | Name of label to traverse.| String | "graph_test" | Required. Must be an existing label. |
| direction | In/ out direction to traverse. | String | "out" | Optional. Default is "out". |
| limit | Number of edges to fetch. | Int | 10 | Optional. Default is 10. |
| offset | Starting position in the index. Used for paginating query results. | Int | 50 | Optional. Default is 0. |
| interval | Filters query results by range on indexed properties. | JSON (dictionary) | `{"from": {"time": 0, "weight": 1}, "to": {"time": 1, "weight": 15}}` | Optional. |
| duration | Specify a time window and filter results by timestamp. | JSON (dictionary) |  `{"from": 1407616431000, "to": 1417616431000}` |Optional. |
| scoring | Scoring factors for property values in query results. These weights will be multiplied to corresponding property values, so that the query results can be sorted by the weighted sum. | JSON (dictionary) |  `{"time": 1, "weight": 2}` |Optional. |
| where | Filtering condition. Think of it as the "WHERE" clause of a SQL query. <br> Currently, a selection of logical operators (**and/ or**) and relational operators ("equal(=)/ sets(in)/ range(between x and y)") are supported. <br> **Do not use any quotation marks for string type** | String | "((_from = 123 and _to = abcd) or gender = M) and is_hidden = false and weight between 1 and 10 or time in (1, 2, 3)". <br> Note that it only supports **long, string, and boolean types.**|Optional. |
|outputField|The usual "to" field of the result edges will be replace with a property field specified by this option.| String | "outputField": "service_user_id". This will output edge's "to" field into "service_user_id" property. | Optional. |
|exclude| For a label with exclude "true", resulting vertices of a traverse will act as a blacklist and be excluded from other labels' traverse results in the same step. | Boolean |  "true"| Optional. Default is "false". |
|include | A Label with include "true" will ignore any exclude options in the step and the traverse results of the label guaranteed be included in the step result. | Boolean | "true" | Optional. Default is "false" |
| duplicate | Whether or not to allow duplicate edges; duplicate edges meaning edges with identical (from, to, label, direction) combination. | String <br> One of "first", "sum", "countSum", or "raw". | "first" | Optional. "**first**" means that only first occurrence of edge survives. <br> "**sum**" means that you will sum up all scores of same edges but only one edge survive. <br>"**countSum**" means to counts the occurrences of same edges but only one edge survives. <br>"**raw**" means that you will allow duplicates edges. Default is "**first**". |
| rpcTimeout | Timeout for the request. | Integer | 100 | Optional. In milliseconds. Default is 100 and maximum is 1000. |
| maxAttempt | Max number of HBase read attempts. | Integer |1 | Optional. Default is 1, and maximum is 5. |
| _to | Specify desired target vertex. Step results will only show edges to given vertex. | String | some vertex id|  Optional. |
| threshold | Score threshold for query results. Edges with lesser scores will be dropped. | Double | 1.0 | Optional. Default is 0.0. |
| transform | "_to" values on resulting edges can be transformed to another value from their properties. | JSON (array of array) | optional, default [ ["_to"]] |


### 6.2. Query API ###

#### 6.2.1. Edge Queries ####
S2Graph provides a query DSL which has been reported to have a pretty steep learning curve.
One tip is to try to understand each features by projecting it to that of a RDBMS such MySQL. This doesn't work all the time, but there are many similarities between S2Graph and a conventional RDBMS.
For example, S2Graphs "getEdges" is used to fetch data and traverse multiple steps. This is very similar to the "SELECT" query in MySQL.
Another tip is to not be shy to ask! Send us an E-mail or open an issue with the problems that you're having with S2Graph.

##### 1. POST /graphs/getEdges #####
Select edges with query.

##### 2. POST /graphs/checkEdges #####
Return edges that connect a given vertex pair.

#### 6.2.2 getEdges Examples ####

##### 1. Duplicate Policy #####
Here is a very basic query to fetch all edges that start from source vertex "101".
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

**Notice the "duplicate" field**. If a target label's consistency level is **weak** and multiple edges exist with the same (from, to, label, direction) id, then the query is expect to have a policy for handling edge duplicates. S2Graph provides four duplicate policies on edges.
>1. raw: Allow duplicates and return all edges.
>2. **first**: Return only the first edge if multiple edges exist. This is default.
>3. countSum: Return only one edge, and return how many duplicates exist.
>4. sum: Return only one edge, and return sum of the scores.


With duplicate "raw", there are actually three edges with the same (from, to, label, direction) id.
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

Duplicate "countSum" returns only one edge with the score sum of 3.
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

##### 2. Select Option Example #####
In case you want to control the fields shown in the result edges, use the "select" option.
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

S2Graph will return only those fields in the result.

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

Default value of the "select" option is an empty array which means that all edge fields are returned.

##### 3. groupBy Option Example #####

Result edges can be grouped by a given field.
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

You can see the result edges are grouped by their "from", "to", and "label" fields.
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
You can also run two queries concurrently, and filter the result of one query with the result of the other.

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
S2Graph will run two concurrent queries, one in the main step, and another in the filter out clause.
Here is more practical example.

```
{
  "filterOut": {
    "srcVertices": [
      {
        "columnName": "uuid",
        "id": "Alec",
        "serviceName": "daumnews"
      }
    ],
    "steps": [
      {
        "step": [
          {
            "direction": "out",
            "label": "daumnews_user_view_news",
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
      "id": "Alec",
      "serviceName": "daumnews"
    }
  ],
  "steps": [
    {
      "nextStepLimit": 10,
      "step": [
        {
          "direction": "out",
          "duplicate": "scoreSum",
          "label": "daumnews_user_view_news",
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
          "label": "daumnews_news_belongto_category",
          "limit": 1
        }
      ]
    },
    {
      "step": [
        {
          "direction": "in",
          "label": "daumnews_news_belongto_category",
          "limit": 10
        }
      ]
    }
  ]
}
```

The main query from the above will traverse a graph of users and news articles as follows:
1. Fetch the list of news articles that user Alec read.
2. Get the categories of the result edges of step one.
3. Fetch other articles that were published in same category.

Meanwhile, Alec does not want to get articles that he already read. This can be taken care of with the following query in the filterOut option:

Articles that Alec has already read.
```
{
    "size": 5,
    "degrees": [
        {
            "from": "Alec",
            "label": "daumnews_user_view_news",
            "direction": "out",
            "_degree": 6
        }
    ],
    "results": [
        {
            "cacheRemain": -19,
            "from": "Alec",
            "to": 20150803143507760,
            "label": "daumnews_user_view_news",
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
            "from": "Alec",
            "to": 20150803150406010,
            "label": "daumnews_user_view_news",
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
            "from": "Alec",
            "to": 20150803144908340,
            "label": "daumnews_user_view_news",
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
            "from": "Alec",
            "to": 20150803124627492,
            "label": "daumnews_user_view_news",
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
            "from": "Alec",
            "to": 20150803113311090,
            "label": "daumnews_user_view_news",
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

Without "filterOut"
```
{
    "size": 2,
    "degrees": [
        {
            "from": 1028,
            "label": "daumnews_news_belongto_category",
            "direction": "in",
            "_degree": 2
        }
    ],
    "results": [
        {
            "cacheRemain": -33,
            "from": 1028,
            "to": 20150803105805092,
            "label": "daumnews_news_belongto_category",
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
            "label": "daumnews_news_belongto_category",
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


with "filterOut"
```
{
    "size": 1,
    "degrees": [],
    "results": [
        {
            "cacheRemain": 85957406,
            "from": 1028,
            "to": 20150803105805092,
            "label": "daumnews_news_belongto_category",
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

Note that article **20150803143507760** has been filtered out.


##### 5. nextStepLimit Example #####
S2Graph provides step-level aggregation so that users can take the top K items from the aggregated results.

##### 6. nextStepThreshold Example #####

##### 7. sample Example #####
```
curl -XPOST localhost:9000/graphs/getEdges -H 'Content-Type: Application/json' -d '
{
    "srcVertices": [{"serviceName": "s2graph", "columnName": "account_id", "id":1}],
    "steps": [
      {"sample":2,"step": [{"label": "graph_test", "direction": "out", "offset": 0, "limit": 10, "scoring": {"time": 1, "weight": 1}}]}
    ]
}
```

##### 8. transform Example #####
With typical two-step query, S2Graph will start the second step from the "_to" (vertex id) values of the first steps' result.
With the "transform" option, you can actually use any single field from the result edges' properties of step one.

Add a "transform" option to the query from example 1.

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

Note that we have six resulting edges.
We have two transform rules, the first one simply fetches edges with their target vertex IDs (such as "to": "10"), and the second rule will fetch the same edges but with the "time" values replacing vertex IDs (such as "to": "to": "time.-30").
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

##### 9. Two-Step Traversal Example #####

The following query will fetch a user's (id 1) friends of friends by chaining multiple steps:

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
##### 10. Three-Step Traversal Example #####

Add more steps for wider traversals. Be gentle on the limit options since the number of visited edges will increase exponentially and become very heavy on the system.


##### 11. More examples #####
Example 1. From label "graph_test", select the first 100 edges that start from vertex "account_id = 1", with default sorting.

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

Example 2. Now select between the 50th and 100th edges from the same query.

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

Example 3. Now add a time range filter so that you will only get the edges that were inserted between 1416214118000 and 1416300000000.

```javascript
curl -XPOST localhost:9000/graphs/getEdges -H 'Content-Type: Application/json' -d '
{
    "srcVertices": [{"serviceName": "s2graph", "columnName": "account_id", "id":1}],
    "steps": [
      [{"label": "graph_test", "direction": "in", "offset": 50, "limit": 50, "duration": {"from": 1416214118000, "to": 1416300000000}]
    ]
}
'
```

Example 4. Now add scoring rule to sort the result by indexed properties "time" and "weight", with weights of 1.5 and 10, respectively.


```javascript
curl -XPOST localhost:9000/graphs/getEdges -H 'Content-Type: Application/json' -d '
{
    "srcVertices": [{"serviceName": "s2graph", "columnName": "account_id", "id":1}],
    "steps": [
      [{"label": "graph_test", "direction": "in", "offset": 50, "limit": 50, "duration": {"from": 1416214118000, "to": 1416214218000}, "scoring": {"time": 1.5, "weight": 10}]
    ]
}
'
```

Example 5. Make a two-step query to fetch friends of friends of a user "account_id = 1". (Limit the first step by 10 friends and the second step by 100.)

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

Example 6. Make a two-step query to fetch the music playlist of the friends of user "account_id = 1". Limit the first step by 10 friends and the second step by 100 tracks.)

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

Example 7. Query the friends of user "account_id = 1" who played the track "track_id = 200".
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

Example 8. See if a list of edges exist in the graph.

```javascript
curl -XPOST localhost:9000/graphs/checkEdges -H 'Content-Type: Application/json' -d '
[
	{"label": "talk_friend", "direction": "out", "from": 1, "to": 100},
	{"label": "talk_friend", "direction": "out", "from": 1, "to": 101}
]
'
```


#### 6.2.3. Vertex Queries ####

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




## 7. Bulk Loading ##

In many cases, the first step to start using S2Graph in production is to migrate a large dataset into S2Graph. S2Graph provides a bulk loading script for importing the initial dataset.

To use bulk load, you need a running [Spark](https://spark.apache.org/) cluster and **TSV file** that follows the S2Graph bulk load format.

Note that if you don't need additional properties on vertices(i.e., you only need vertex id), you only need to publish the edges and not the vertices. Publishing edges will create vertices with empty properties by default.

#### Edge Format

|Timestamp | Operation | Log Type |  From | To | Label | Props |
|:------- | --- |:----: | --- | -----| --- | --- |
|1416236400000|insert|edge|56493|26071316|talk_friend_long_term_agg_by_account_id|{"timestamp":1416236400000,"score":0}|

#### Vertex Format

|Timestamp | Operation | Log Type |  ID | Service Name | Column Name | Props |
|:------- | --- |:----: | --- | -----| --- | --- |
|1416236400000|insert|vertex|56493|kakaotalk|account_id|`{"is_active":true, "country_iso": "kr"}`|

### Build ###
In order to build the loader, run following command.

`> sbt "project loader" "clean" "assembly"`

This will give you  **s2graph-loader-assembly-X.X.X-SNAPSHOT.jar** under loader/target/scala-2.xx/

### Source Data Storage Options

For bulk loading, source data can be either in HDFS or a Kafka queue.

#### 1. For source data in HDFS ###

 - Run subscriber.GraphSubscriber to upload the TSV file into S2Graph.
 - From the Spark UI, you can check if the number of stored edges are correct.

#### 2. For source data is in Kafka ####

This requires the data to be in the bulk loading format and streamed into a Kafka cluster.

 - Run subscriber.GraphSubscriberStreaming to stream-load into S2Graph from a kafka topic.
 - From the Spark UI, you can check if the number of stored edges are correct.

#### 3. online migration ####
The following explains how to run an online migration from RDBMS to S2Graph. assumes that client send same events that goes to primary storage(RDBMS) and S2Graph.

 - Set label as isAsync "true". This will redirect all operations to a Kafka queue.
 - Dump your RDBMS data to a TSV for bulk loading.
 - Load TSV file with subscriber.GraphSubscriber.
 - Set label as isAsync "false". This will stop queuing events into Kafka and make changes into S2Graph directly.
 - Run subscriber.GraphSubscriberStreaming to queued events. Because the data in S2Graph is idempotent, it is safe to replay queued message while bulk load is still in process.


## 8. Benchmark ##


### Test Data
1. A synthetic dense matrix(10 million row x 1000 column, total edge 10 billion) data set.
2. Number of HBase region servers: 20
3. Number of S2Graph instances: 1

#### 1. One-Step Query
```
{
    "srcVertices": [
        {
            "serviceName": "test",
            "columnName": "user_id",
            "id": %s
        }
    ],
    "steps": [
      [
        {
          "label": "friends",
          "direction": "out",
          "offset": 0,
          "limit": %d
        }
      ]
    ]
}
```
| Number of S2Graph Servers |  VUsers | Offset | 1st Step Limit | TPS | Latency |
|:------- | --- |:----: | --- | --- | --- | --- |
| 1 | 20 | 0 | 100 | 3110.3TPS | 6.25ms |
| 1 | 20 | 0 | 200 | 2,595.3TPS | 7.52 ms |
| 1 | 20 | 0 | 400 | 1,449.8TPS | 13.56ms |
| 1 | 20 | 0 | 800 | 789.4TPS | 25.14ms |

<img width="884" alt="screen shot 2015-09-03 at 11 50 59 am" src="https://cloud.githubusercontent.com/assets/1264825/9649651/ce3424f4-5232-11e5-8350-4ac0c5e5a523.png">

#### 2. Two-Step query
```
{
    "srcVertices": [
        {
            "serviceName": "test",
            "columnName": "user_id",
            "id": %s
        }
    ],
    "steps": [
      [
        {
          "label": "friends",
          "direction": "out",
          "offset": 0,
          "limit": %d
        }
      ],
      [
        {
          "label": "friends",
          "direction": "out",
          "offset": 0,
          "limit": %d
        }
      ]
    ]
}
```
| Number of S2Graph Servers |  VUsers | 1st Step Limit | 2nd Step Limit | TPS | Latency |
|:------- | --- |:----: | --- | --- | --- | --- |  
| 1 | 20 | 10 | 10 | 3,050.3TPS | 6.43ms  |
| 1 | 20 | 10 | 20 | 2,239.3TPS | 8.8 ms |
| 1 | 20 | 10 | 40 | 1,393.4TPS | 14.19ms |
| 1 | 20 | 10 | 60 | 1,052.2TPS | 18.83ms |
| 1 | 20 | 10 | 80 | 841.2TPS | 23.59ms |
| 1 | 20 | 10 | 100 | 700.3TPS | 28.34ms |
| 1 | 20 | 10 | 200 | 397.8TPS | 50.03ms |
| 1 | 20 | 10 | 400 | 256.9TPS | 77.62ms |
| 1 | 20 | 10 | 800 | 192TPS | 103.97ms |
| 1 | 20 | 20 | 10 | 1,820.8TPS | 10.83ms |
| 1 | 20 | 20 | 20 | 1,252.8TPS | 15.78ms |
| 1 | 20 | 40 | 10 | 1,022.8TPS | 19.36ms |
| 1 | 20 | 60 | 10 | 732.8TPS | 27.11ms |
| 1 | 20 | 80 | 10 | 570.1TPS | 34.87ms |
| 1 | 20 | 100 | 10 | 474.9TPS | 41.87ms |
| 1 | 20 | 100 | 10  | 288.5TPS | 68.34ms |
| 1 | 20 | 100 | 20 |  279.1TPS | 71.4ms |
| 1 | 20 | 100 | 40 | 156.6TPS | 127.43ms |
| 1 | 20 | 100 | 80 | 83.6TPS | 238.48ms |
| 1 | 20 | 200 | 10 | 236.8TPS | 84.16ms |
| 1 | 20 | 400 | 10 | 121.8TPS | 163.87ms |
| 1 | 20 | 800 | 10 | 60.9TPS | 327.23ms |
| 1 | 10 | 800 | 10 | 61.8TPS | 162.19 ms |
| 1 | 5 | 800 | 10 | 54.2TPS | 91.87ms |
| 1 | 20 | 800 | 1 | 181.4TPS | 110ms |

<img width="903" alt="screen shot 2015-09-03 at 11 51 06 am" src="https://cloud.githubusercontent.com/assets/1264825/9649652/ce34abf4-5232-11e5-94d3-51a231d508a4.png">
<img width="892" alt="screen shot 2015-09-03 at 11 51 13 am" src="https://cloud.githubusercontent.com/assets/1264825/9649653/ce35c3a4-5232-11e5-8cdb-4bf220dc2cae.png">

#### 3. Three-Step query
```
{
    "srcVertices": [
        {
            "serviceName": "test",
            "columnName": "user_id",
            "id": %s
        }
    ],
    "steps": [
      [
        {
          "label": "friends",
          "direction": "out",
          "offset": 0,
          "limit": %d
        }
      ],
      [
        {
          "label": "friends",
          "direction": "out",
          "offset": 0,
          "limit": %d
        }
      ],
      [
        {
          "label": "friends",
          "direction": "out",
          "offset": 0,
          "limit": %d
        }
      ]
    ]
}
```
| Number of S2Graph Servers |  VUsers | 1st Step Limit | 2nd Step Limit | 3rd Step Limit | TPS | Latency |
|:------- | --- |:----: | --- | --- | --- | --- | --- |  
| 1 | 20 | 10 | 10 | 10 | 325TPS | 61.14ms |
| 1 | 20 | 10 | 10 | 20 | 189TPS | 105.31ms |
| 1 | 20 | 10 | 10 | 40 | 95TPS | 209.7ms |
| 1 | 20 | 10 | 10 | 80 | 27TPS | 727.56ms |

<img width="883" alt="screen shot 2015-09-03 at 11 51 19 am" src="https://cloud.githubusercontent.com/assets/1264825/9649654/ce372136-5232-11e5-9073-d9fbc37e0e78.png">

## 8. Resources ##
* [HBaseCon](http://hbasecon.com/agenda)
  * HBaseCon2015: S2Graph - A Large-scale Graph Database with HBase
     * [presentation](https://vimeo.com/128203919)
     * [slide](http://www.slideshare.net/HBaseCon/use-cases-session-5)
* Mailing List: Use [google group](https://groups.google.com/forum/#!forum/s2graph) or fire issues on this repo.
* Contact: shom83@gmail.com




[![Analytics](https://ga-beacon.appspot.com/UA-62888350-1/s2graph/readme.md)](https://github.com/daumkakao/s2graph)
