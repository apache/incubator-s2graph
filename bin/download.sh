#!/usr/bin/env bash
mkdir -p s2core/lib
mkdir -p s2counter_core/lib

wget https://github.com/SteamShon/asynchbase/raw/mvn-repo/org/hbase/asynchbase/1.7.2-S2GRAPH/asynchbase-1.7.2-S2GRAPH-jar-with-dependencies.jar

mv asynchbase-1.7.2-S2GRAPH-jar-with-dependencies.jar s2core/lib

wget http://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-java-5.1.39.tar.gz

tar xvzf mysql-connector-java-5.1.39.tar.gz

cp mysql-connector-java-5.*/mysql-connector-java-5.*-bin.jar s2core/lib/
cp mysql-connector-java-5.*/mysql-connector-java-5.*-bin.jar s2counter_core/lib/


rm -rf mysql-connector-java-5.*