# S2Graph Tinkerpop3 Implementation(s2graph-gremlin)

Currently tested with tinkerpop 3.2.5 only on gremlin-console.

## Requirement

1. Download latest [Apache Tinkerpop 3.2.5](https://www.apache.org/dyn/closer.lua/tinkerpop/3.2.5/apache-tinkerpop-gremlin-console-3.2.5-bin.zip).
2. set environment variable `GREMLIN_HOME`.
3. create ~/.groovy/grapeConfig.xml file if it does not exist as follow.

```
<ivysettings>
  <settings defaultResolver="downloadGrapes"/>
  <resolvers>
    <chain name="downloadGrapes">
      <filesystem name="cachedGrapes">
        <ivy pattern="${user.home}/.groovy/grapes/[organisation]/[module]/ivy-[revision].xml"/>
        <artifact pattern="${user.home}/.groovy/grapes/[organisation]/[module]/[type]s/[artifact]-[revision].[ext]"/>
      </filesystem>
      <ibiblio name="local" root="file:${user.home}/.m2/repository/" m2compatible="true"/>
      <ibiblio name="codehaus" root="http://repository.codehaus.org/" m2compatible="true"/>
      <ibiblio name="central" root="http://central.maven.org/maven2/" m2compatible="true"/>
      <ibiblio name="jitpack" root="https://jitpack.io" m2compatible="true"/>
      <ibiblio name="java.net2" root="http://download.java.net/maven/2/" m2compatible="true"/>
    </chain>
  </resolvers>
</ivysettings>
```

## Build

following is how to setup this project on m2 repository.

1. `sbt "project s2graph_gremlin" publishM2`: this will create single fat jar under m2 repository.
2. check if `GREMLIN_HOME` is correct.
3. goto `cd s2graph_gremlin/examples`.
4. install s2graph-gremlin plugin, `sh install_plugin.sh`.
5. go to `${GREMLIN_HOME}/bin/gremlin.sh`
5. try `s2graph_modern.groovy` to play with modern graph comes with tinkerpop.
6. try `s2graph_getting_started.groovy` for s2graph specific methods.
 
 