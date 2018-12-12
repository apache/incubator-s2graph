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

# Run S2Graph using Docker

1. Build a docker image of the s2graph in the project's root directory
    - you can build images for each type of API Server
        ```
        // s2http
        sbt "project s2http" 'set version := "latest"' docker
        ```

    - find local image is created correctly by using `docker images`

    - (optional) If you need to add extra jars in classpath, use environment variable 'EXTRA_JARS'
        ```
        docker run --name s2graph -v /LocalJarsDir:/extraJars -e EXTRA_JARS=/extraJars -dit s2graph/s2graphql:latest ...
        ```

2. Run MySQL and HBase container first.
    - change directory to dev-support. `cd dev_support`
    - `docker-compose build`
3. Run graph container
    - `docker-compose up -d`

> S2Graph should be connected with MySQL at initial state. Therefore you have to run MySQL and HBase before running it.

## For OS X

In OS X, the docker container is running on VirtualBox. In order to connect with HBase in the docker container from your local machine. You have to register the IP of the docker-machine into the `/etc/hosts` file.

Within the `docker-compose.yml` file, I had supposed the name of docker-machine as `default`. So, in the `/etc/hosts` file, register the docker-machine name as `default`.

```
ex)
192.168.99.100 default
```

# Run S2Graph on your local machine

In order to develop and test S2Graph. You might be want to run S2Graph as `dev` mode on your local machine. In this case, the following commands are helpful.

- Run only MySQL and HBase

```
# docker-compose up -d graph_mysql
```

- Run s2graph as 'dev' mode

```
# sbt "project s2http" run -Dhost=default
```

- or run test cases

```
# sbt test -Dhost=default
```
