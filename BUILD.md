## Build From Source

1. An [SBT](http://www.scala-sbt.org/) installation
> `brew install sbt` if you are on a Mac. (Otherwise, checkout the [SBT document](http://www.scala-sbt.org/0.13/docs/Manual-Installation.html).

2. Download third_party dependencies.
    3. download [mysql-jdbc-connector](http://dev.mysql.com/downloads/connector/j/) and locate it into `s2core/lib` and `s2counter_core`
    4. download [patched-asynchbase](https://github.com/SteamShon/asynchbase/blob/mvn-repo/org/hbase/asynchbase/1.7.2-S2GRAPH/asynchbase-1.7.2-S2GRAPH-jar-with-dependencies.jar) and locate it into `s2core/lib`

2. build and package from source code.
> `sbt package` to create a package at the directory `target/deploy`

once build is done, `target/s2graph-0.1.0-incubating-bin` will be created.

then `bin/start-s2graph.sh` will launch the following.

1. S2Graph server.
2. Standalone HBase server as the data storage.
3. H2 as metastore.
