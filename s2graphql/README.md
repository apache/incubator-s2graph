# Suggest to implement GraphQL as standard web interface for S2Graph.

  - To support GraphQL i used [Akka HTTP](https://github.com/akka/akka-http) and [Sangria](https://github.com/sangria-graphql). each is an HTTP Server and GraphQL Scala implementation.
  - I also used [GraphiQL](https://github.com/graphql/graphiql) as a tool for GraphQL queries.

## Wroking example

![mutation](https://user-images.githubusercontent.com/1182522/35611013-f551f2b6-06a6-11e8-8f48-e39e667a8849.gif)

![query](https://user-images.githubusercontent.com/1182522/35611725-599e1e5a-06a9-11e8-9a52-9e5fd3542c2e.gif)


## Overview
  
  The reason I started this work is because the `Label` used by S2Graph has a strong type system, so I think it will work well with the `schema` provided by GraphQL.
  
  To do this, we converted S2Graph Model (Label, Service ...) into GraphLQL schema whenever added (changed).

## Setup
  Assume that hbase is running on localhost.  
  If the hbase environment is not set up, you can run it with the following command

```bash
sbt package
target/apache-s2graph-0.2.1-SNAPSHOT-incubating-bin/bin/hbase-standalone.sh start 
```
  
If hbase is running well, run the following command after cloning the project locally.

`GraphiQL` is not directly related to the `GraphQL` implementation, but is recommended for convenient queries.
Because of the license problem, you should download the file through the following command.

```bash
cd s2graphql/src/main/resources
wget https://raw.githubusercontent.com/sangria-graphql/sangria-akka-http-example/master/src/main/resources/graphiql.html
```

You can see that the `graphiql.html` file is added to the `s2graphql/src/main/resources` folder as shown below.

```
$ls
application.conf  graphiql.html log4j.properties
```

And let's run http server.

```bash
sbt -DschemaCacheTTL=-1 -Dhttp.port=8000 'project s2graphql' '~re-start'
```

When the server is running, connect to `http://localhost:8000`. If it works normally, you can see the following screen.

![2018-01-31 4 39 25](https://user-images.githubusercontent.com/1182522/35610627-5ddd1cd6-06a5-11e8-8f02-446b28df54cb.png)

## API List
  - createService
  - createLabel
  - addEdges
  - addEdge
  - query (You can recursively browse the linked labels from the service and any other labels that are linked from that label)

## Your First Grpah (GraphQL version)

[S2Graph tutorial](https://github.com/apache/incubator-s2graph#your-first-graph)
I have ported the contents of `Your first graph` provided by S2Graph based on GraphQL.

### Start by connecting to `http://localhost:8000`.

The environment for this example is Mac OS and Chrome.
You can get help with schema-based `Autocompletion` using the `ctrl + space` key.

If you add a `label` or `service`, you will need to `refresh` (`cmd + r`) your browser because the schema will change dynamically.

1. First, we need a name for the new service.

    The following POST query will create a service named "KakaoFavorites".

Request 
```graphql
mutation {
  createService(
    name: "KakaoFavorites",
    compressionAlgorithm: gz
  ) {
    isSuccess
    message
    created {
      id
    }
  } 
}
```

Response
```json
{
  "data": {
    "createService": {
      "isSuccess": true,
      "message": "Created successful",
      "created": {
        "id": 1
      }
    }
  }
}
```

To make sure the service is created correctly, check out the following.

  > Since the schema has changed, GraphiQL must recognize the changed schema. To do this, refresh the browser several times.

Request
```graphql
query {
  Services(name: KakaoFavorites) {
    id
    name     
  }
}
```

Response
```json
{
  "data": {
    "Services": [
      {
        "id": 1,
        "name": "KakaoFavorites"
      }
    ]
  }
}
```

2. Next, we will need some friends.

    In S2Graph, relationships are organized as labels. Create a label called friends using the following createLabel API call:

Request 

```graphql
mutation {
  createLabel(
    name: "friends",
    sourceService: {
      name: KakaoFavorites,
      columnName: "userName",
      dataType: string
    },
    targetService: {
      name: KakaoFavorites,
      columnName: "userName",
      dataType: string
    }
    consistencyLevel: strong
  ){
    isSuccess
    message
    created {
      id
      name      
    }
  }
}
```

Response 
```json
{
  "data": {
    "createLabel": {
      "isSuccess": true,
      "message": "Created successful",
      "created": {
        "id": 1,
        "name": "friends"
      }
    }
  }
}
```

Check if the label has been created correctly
> Since the schema has changed, GraphiQL must recognize the changed schema. To do this, refresh the browser several times.

Request
```graphql
query { 
  Labels(name: friends) {
    id
    name
    srcColumnName
    tgtColumnName    
  }
}
```

Response
```json
{
  "data": {
    "Labels": [
      {
        "id": 1,
        "name": "friends",
        "srcColumnName": "userName",
        "tgtColumnName": "userName"
      }
    ]
  }
}
```

Now that the label friends is ready, we can store the friendship data. 
Entries of a label are called edges, and you can add edges with edges/insert API:

> Since the schema has changed, GraphiQL must recognize the changed schema. To do this, refresh the browser several times.

Request
```graphql
mutation {
  addEdges(
    friends: [
      {from: "Elmo", to: "Big Bird"},
      {from: "Elmo", to: "Ernie"},    
      {from: "Elmo", to: "Bert"},    
      {from: "Cookie Monster", to: "Grover"},    
      {from: "Cookie Monster", to: "Kermit"},    
      {from: "Cookie Monster", to: "Oscar"},    
    ]
  ) {
    isSuccess    
  }
}
```

Response
```json
{
  "data": {
    "addEdges": [
      {
        "isSuccess": true
      },
      {
        "isSuccess": true
      },
      {
        "isSuccess": true
      },
      {
        "isSuccess": true
      },
      {
        "isSuccess": true
      },
      {
        "isSuccess": true
      }
    ]
  }
}
```

Query friends of Elmo with getEdges API:

Request

```graphql
query {
  KakaoFavorites(id: "Elmo") {    
    friends {
      to
    }
  }
}
```

Response

```json
{
  "data": {
    "KakaoFavorites": [
      {
        "friends": [
          {
            "to": "Bert"
          },
          {
            "to": "Ernie"
          },
          {
            "to": "Big Bird"
          }
        ]
      }
    ]
  }
}
```

Now query friends of Cookie Monster:

```graphql
query {
  KakaoFavorites(id: "Cookie Monster") {
    friends {
      to
    }
  }
}
```

3. Users of Kakao Favorites will be able to post URLs of their favorite websites.

Request

```graphql
mutation {
  createLabel(
    name: "post",
    sourceService: {
      name: KakaoFavorites,
      columnName: "userName",
      dataType: string
    },
    targetService: {
      name: KakaoFavorites,
      columnName: "url",
      dataType: string,      
    }
    consistencyLevel: strong
  ) {
    isSuccess
    message
    created {
      id
      name      
    }
  }
}
```

Response

```json
{
  "data": {
    "createLabel": {
      "isSuccess": true,
      "message": "Created successful",
      "created": {
        "id": 2,
        "name": "post"
      }
    }
  }
}
```

Now, insert some posts of the users:

> Since the schema has changed, GraphiQL must recognize the changed schema. To do this, refresh the browser several times.


Request

```graphql
mutation {
  addEdges(
    post: [
      { from: "Big Bird", to: "www.kakaocorp.com/en/main" },
      { from: "Big Bird", to: "github.com/kakao/s2graph" },
      { from: "Ernie", to: "groups.google.com/forum/#!forum/s2graph" },
      { from: "Grover", to: "hbase.apache.org/forum/#!forum/s2graph" },
      { from: "Kermit", to: "www.playframework.com"},
      { from: "Oscar", to: "www.scala-lang.org"}
    ]
  ) {
    isSuccess
  }
}
```

Response
```json
{
  "data": {
    "addEdges": [
      {
        "isSuccess": true
      },
      {
        "isSuccess": true
      },
      {
        "isSuccess": true
      },
      {
        "isSuccess": true
      },
      {
        "isSuccess": true
      },
      {
        "isSuccess": true
      }
    ]
  }
}
```

4. So far, we have designed a label schema for the labels friends and post, and stored some edges to them.+

    This should be enough for creating the timeline feature! The following two-step query will return the URLs for Elmo's timeline, which are the posts of Elmo's friends:

Request

```graphql
query {
  KakaoFavorites(id: "Elmo") {
    friends {
      post {
        from
        to
      }
    }
  }
}
```

Response
```json
{
  "data": {
    "KakaoFavorites": [
      {
        "friends": [
          {
            "post": []
          },
          {
            "post": [
              {
                "from": "Ernie",
                "to": "groups.google.com/forum/#!forum/s2graph"
              }
            ]
          },
          {
            "post": [
              {
                "from": "Big Bird",
                "to": "www.kakaocorp.com/en/main"
              },
              {
                "from": "Big Bird",
                "to": "github.com/kakao/s2graph"
              }
            ]
          }
        ]
      }
    ]
  }
}
```

Also try Cookie Monster's timeline:

Request
```graphql
query {
  KakaoFavorites(id: "Cookie Monster") {
    friends {
      post {
        from
        to
      }
    }
  }
}
```

Response
```json
{
  "data": {
    "KakaoFavorites": [
      {
        "friends": [
          {
            "post": [
              {
                "from": "Oscar",
                "to": "www.scala-lang.org"
              }
            ]
          },
          {
            "post": [
              {
                "from": "Kermit",
                "to": "www.playframework.com"
              }
            ]
          },
          {
            "post": [
              {
                "from": "Grover",
                "to": "hbase.apache.org/forum/#!forum/s2graph"
              }
            ]
          }
        ]
      }
    ]
  }
}
```


![2018-01-31 5 18 46](https://user-images.githubusercontent.com/1182522/35612101-db97e160-06aa-11e8-9286-0dd1ffa15c82.png)

The example above is by no means a full blown social network timeline, but it gives you an idea of how to represent, store and query graph data with S2Graph.

