
## S2Jobs

S2Jobs is a collection of spark programs that connect S2Graph `WAL` to other systems.


## Background

By default, S2Graph publish all incoming data as `WAL` to Apache Kafka for users who want to subscribe `WAL`.

There are many use cases of this `WAL`, but let's just start with simple example, such as **finding out the number of new edges created per minute(OLAP query).**

One possible way is run full table scan on HBase using API, then group by each edge's `createdAt` property value, then count number of edges per each `createdAt` bucket, in this case minute. 

Running full table scan on HBase through RegionServer on same cluster that is serving lots of concurrent OLTP requests is prohibit, arguably.

Instead one can subscribe `WAL` from kafka, and sink `WAL` into HDFS, which usually separate hadoop cluster from the cluster which run HBase region server for OLTP requests.

Once `WAL` is available in separate cluster as file, by default the Spark DataFrame, answering above question becomes very easy with spark sql. 

```
select		MINUTE(timestamp), count(1) 
from		wal
where		operation = 'insert'
and 		timestamp between (${start_ts}, ${end_ts})
```

Above approach works, but there is usually few minutes of lag. If user want to reduce this lag, then it is also possible to subscribe `WAL` from kafka then ingest data into analytics platform such as Druid. 

S2Jobs intentionaly provide only interfaces and very basic implementation for connecting `WAL` to other system. It is up to users what system they would use for `WAL` and S2Jobs want the community to contribute this as they leverage S2Graph `WAL`.

## Basic Architecture

One simple example data flow would look like following.

<img width="1222" alt="screen shot 2018-03-29 at 3 04 21 pm" src="https://user-images.githubusercontent.com/1264825/38072702-84ef93dc-3362-11e8-9f47-db41f50467f0.png">

Most of spark program available on S2jobs follow following abstraction.

### Task
`Process class` ? `Task trait` ? `TaskConf`?

### Current Supported Task

### Source

-   kakfa : built-in 
-   file : built-in
-   hive : built-in

### Process

-   sql : process spark sql
-   custom : implement if necessary

### Sink

-   kafka : built-in
    
-   file : built-in
    
-   es : elasticsearch-spark
    
-   **s2graph** : added
    
    -   Use the mutateElement function of the S2graph object.
    -   S2graph related setting is required.
    -   put the config file in the classpath or specify it in the job description options.
    
    ```
    ex)
        "type": "s2graph",
        "options": {
          "hbase.zookeeper.quorum": "",
          "db.default.driver": "",
          "db.default.url": ""
        }
    
    ```

#### Data Schema for Kafka

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


----------

### Job Description

**Tasks** and **workflow** can be described in **job** description, and dependencies between tasks are defined by the name of the task specified in the inputs field

>Note that this works was influenced by [airstream of Airbnb](https://www.slideshare.net/databricks/building-data-product-based-on-apache-spark-at-airbnb-with-jingwei-lu-and-liyin-tang).

#### Json Spec

```
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


### Sample job

#### 1. wallog trasnform (kafka to kafka)

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

#### 2. wallog transform (hdfs to hdfs)

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


----------


### Launch Job

When submitting spark job with assembly jar, use these parameters with the job description file path.  
(currently only support file type)

```
// main class : org.apache.s2graph.s2jobs.JobLauncher
Usage: run [file|db] [options]
  -n, --name <value>      job display name
Command: file [options]
get config from file
  -f, --confFile <file>   configuration file
Command: db [options]
get config from db
  -i, --jobId <jobId>     configuration file
```