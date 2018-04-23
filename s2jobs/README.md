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

  
# S2Jobs

S2Jobs is a collection of spark programs which can be used to support `online transaction processing(OLAP)` on S2Graph.

There are currently two ways to run `OLAP` on S2Graph.


----------


## 1. HBase Snapshots

HBase provides excellent support for creating table [snapshot](http://hbase.apache.org/0.94/book/ops.snapshots.html)

S2Jobs provide `S2GraphSource` class which can create `Spark DataFrame` from `S2Edge/S2Vertex` stored in HBase Snapshot.

Instead of providing graph algorithms such as `PageRank` by itself, S2Graph let users connect graph stored in S2Graph to their favorite analytics platform, for example [**`Apache Spark`**](https://spark.apache.org/). 

Once user finished processing, S2Jobs provide `S2GraphSink` to connect analyzed data into S2Graph back.


![screen shot 2018-04-06 at 2 22 28 pm](https://user-images.githubusercontent.com/1264825/38404575-0158844e-39a6-11e8-935f-0a7d971b068b.png)

This architecture seems complicated at the first glace, but note that this approach has lots of advantages on performance and stability on `OLTP` cluster especially comparing to using HBase client API `Scan`.
 
Here is result `DataFrame` schema for `S2Vertex` and `S2Edge`. 

```
S2Vertex
root
 |-- timestamp: long (nullable = false)
 |-- operation: string (nullable = false)
 |-- elem: string (nullable = false)
 |-- id: string (nullable = false)
 |-- service: string (nullable = false)
 |-- column: string (nullable = false)
 |-- props: string (nullable = false)

S2Edge
root
 |-- timestamp: long (nullable = false)
 |-- operation: string (nullable = false)
 |-- elem: string (nullable = false)
 |-- from: string (nullable = false)
 |-- to: string (nullable = false)
 |-- label: string (nullable = false)
 |-- props: string (nullable = false)
 |-- direction: string (nullable = true)
```

To run graph algorithm, transform above `DataFrame` into [GraphFrames](https://graphframes.github.io/index.html), then run provided functionality on `GraphFrames`. 

Lastly, `S2GraphSource` and `S2GraphSink`  open two interface `GraphElementReadable` and `GraphElementWritable` for users who want to serialize/deserialize custom graph from/to S2Graph. 

For example, one can simply implement `RDFTsvFormatReader` to convert each triple on RDF file to `S2Edge/S2Vertex` then use it in `S2GraphSource`'s `toDF` method to create `DataFrame` from RDF. 

This comes very handily when there are many different data sources with different formats to migrate into S2Graph.


## 2. `WAL` log on Kafka

By default, S2Graph publish all incoming data into Kafka, and users subscribe this for **incremental processing**. 

S2jobs provide programs to process `stream` for incremental processing, using [Spark  Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html), which provide a great way to express streaming computation the same way as a batch computation. 

The `Job` in S2Jobs abstract one spark and `Job` consist of multiple `Task`s. Think `Job` as very simple `workflow` and there are `Source`, `Process`, `Sink` subclass that implement `Task` interface. 

----------
### 2.1. Job Description

**Tasks** and **workflow** can be described in **Job** description, and dependencies between tasks are defined by the name of the task specified in the inputs field

>Note that these works were influenced by [airstream of Airbnb](https://www.slideshare.net/databricks/building-data-product-based-on-apache-spark-at-airbnb-with-jingwei-lu-and-liyin-tang).

#### Json Spec

```js
{
    "name": "JOB_NAME",
    "source": [
        {
            "name": "TASK_NAME",
            "inputs": [],
            "type": "SOURCE_TYPE",
            "options": {
                "KEY" : "VALUE"
            }
        }
    ],
    "process": [
        {
            "name": "TASK_NAME",
            "inputs": ["INPUT_TASK_NAME"],
            "type": "PROCESS_TYPE",
            "options": {
                "KEY" : "VALUE"
            }
        }
    ],
    "sink": [
        {
            "name": "TASK_NAME",
            "inputs": ["INPUT_TASK_NAME"],
            "type": "SINK_TYPE",
            "options": {
                "KEY" : "VALUE"
            }
        }
    ]
}

```
----------

### 2.2. Current supported `Task`s.

#### Source

- KafkaSource: Built-in from Spark.

##### Data Schema for Kafka

When using Kafka as data source consumer needs to parse it and later on interpret it, because of Kafka has no schema.

When reading data from Kafka with structure streaming, the Dataframe has the following schema.

```
Column    Type
key        binary
value    binary
topic    string
partition    int
offset    long
timestamp    long
timestampType    int

```

In the case of JSON format, data schema can be supported in config.  
You can create a schema by giving a string representing the struct type as JSON as shown below.

```
{
  "type": "struct",
  "fields": [
    {
      "name": "timestamp",
      "type": "long",
      "nullable": false,
      "metadata": {}
    },
    {
      "name": "operation",
      "type": "string",
      "nullable": true,
      "metadata": {}
    },
    {
      "name": "elem",
      "type": "string",
      "nullable": true,
      "metadata": {}
    },
    {
      "name": "from",
      "type": "string",
      "nullable": true,
      "metadata": {}
    },
    {
      "name": "to",
      "type": "string",
      "nullable": true,
      "metadata": {}
    },
    {
      "name": "label",
      "type": "string",
      "nullable": true,
      "metadata": {}
    },
    {
      "name": "service",
      "type": "string",
      "nullable": true,
      "metadata": {}
    },
    {
      "name": "props",
      "type": "string",
      "nullable": true,
      "metadata": {}
    }
  ]
}

```

- FileSource: Built-in from Spark.
- HiveSource: Built-in from Spark.
- S2GraphSource 
	- HBaseSnapshot read, then create DataFrame. See HBaseSnapshot in this document.
	- Example options for `S2GraphSource` are following(reference examples for details).
    
```js
{
	"type": "s2graph",
	"options": {
		"hbase.zookeeper.quorum": "localhost",
		"db.default.driver": "com.mysql.jdbc.Driver",
		"db.default.url": "jdbc:mysql://localhost:3306/graph_dev",
		"db.default.user": "graph",
		"db.default.password": "graph",
		"hbase.rootdir": "/hbase",
		"restore.path": "/tmp/restore_hbase",
		"hbase.table.names": "movielens-snapshot"
	}
}
```


#### Process
-   SqlProcess : process spark sql
-   custom : implement if necessary

#### Sink

- KafkaSink : built-in from Spark.
- FileSink : built-in from Spark.
- HiveSink: buit-in from Spark.
- ESSink : elasticsearch-spark
- **S2GraphSink**    
   -  writeBatchBulkload: build `HFile` directly, then load it using `LoadIncrementalHFiles` from HBase.
   - writeBatchWithMutate: use the `mutateElement` function of the S2graph object.




----------


The very basic pipeline can be illustrated in the following figure.

![screen shot 2018-04-06 at 5 15 00 pm](https://user-images.githubusercontent.com/1264825/38409873-141dcb6c-39be-11e8-99e3-74e3166d8553.png)


# Job Examples

## 1. `WAL` log trasnform (kafka to kafka)

```
{
    "name": "kafkaJob",
    "source": [
        {
            "name": "wal",
            "inputs": [],
            "type": "kafka",
            "options": {
                "kafka.bootstrap.servers" : "localhost:9092",
                "subscribe": "s2graphInJson",
                "maxOffsetsPerTrigger": "10000",
                "format": "json",
                "schema": "{\"type\":\"struct\",\"fields\":[{\"name\":\"timestamp\",\"type\":\"long\",\"nullable\":false,\"metadata\":{}},{\"name\":\"operation\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"elem\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"from\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"to\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"label\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"service\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"props\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}"
            }
        }
    ],
    "process": [
        {
            "name": "transform",
            "inputs": ["wal"],
            "type": "sql",
            "options": {
                "sql": "SELECT timestamp, `from` as userId, to as itemId, label as action FROM wal WHERE label = 'user_action'"
            }
        }
    ],
    "sink": [
        {
            "name": "kafka_sink",
            "inputs": ["transform"],
            "type": "kafka",
            "options": {
                "kafka.bootstrap.servers" : "localhost:9092",
                "topic": "s2graphTransform",
                "format": "json"
            }
        }
    ]
}

```

## 2. `WAL` log transform (HDFS to HDFS)

```
{
    "name": "hdfsJob",
    "source": [
        {
            "name": "wal",
            "inputs": [],
            "type": "file",
            "options": {
                "paths": "/wal",
                "format": "parquet"
            }
        }
    ],
    "process": [
        {
            "name": "transform",
            "inputs": ["wal"],
            "type": "sql",
            "options": {
                "sql": "SELECT timestamp, `from` as userId, to as itemId, label as action FROM wal WHERE label = 'user_action'"
            }
        }
    ],
    "sink": [
        {
            "name": "hdfs_sink",
            "inputs": ["transform"],
            "type": "file",
            "options": {
                "path": "/wal_transform",
                "format": "json"
            }
        }
    ]
}

```

## 3. movielens (File to S2Graph)

You can also run an example job that parses movielens data and writes to S2graph.
The dataset includes user rating and tagging activity from MovieLens(https://movielens.org/), a movie recommendation service. 

```
// move to example folder
$ cd ../example

// run example job 
$ ./run.sh movielens
```

It demonstrate how to build a graph-based data using the publicly available MovieLens dataset on graph database S2Graph,
and provides an environment that makes it easy to use various queries using GraphQL.

----------


## Launch Job

When submitting spark job with assembly jar, use these parameters with the job description file path.  
(currently only support file type)

```
// main class : org.apache.s2graph.s2jobs.JobLauncher
Usage: run [file|db] [options]
         -n, --name <value>      job display name
Command: file [options]          get config from file
         -f, --confFile <file>   configuration file
Command: db [options]            get config from db
         -i, --jobId <jobId>     configuration file
```

For example, you can run your application using spark-submit as shown below.
```
$ sbt 'project s2jobs' assembly
$ ${SPARK_HOME}/bin/spark-submit \
    --class org.apache.s2graph.s2jobs.JobLauncher \
    --master local[2] \
    s2jobs/target/scala-2.11/s2jobs-assembly-0.2.1-SNAPSHOT.jar file -f JOB_DESC.json -n JOB_NAME
```


