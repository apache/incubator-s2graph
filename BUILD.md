## Build From Source

1. An [SBT](http://www.scala-sbt.org/) installation
> `brew install sbt` if you are on a Mac. (Otherwise, checkout the [SBT document](http://www.scala-sbt.org/0.13/docs/Manual-Installation.html).

2. Download third_party dependencies.
    1. run `bin/download.sh`.
    
2. build and package from source code.
> `sbt package` to create a package at the directory `target/`

once build is done, `target/s2graph-0.1.0-incubating-bin` will be created.

then `target/s2graph-0.1.0-incubating-bin/bin/start-s2graph.sh` will launch the following.

1. S2Graph server.
2. Standalone HBase server as the data storage.
3. H2 as metastore.
