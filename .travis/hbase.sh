#!/usr/bin/env bash

wget -q -O - https://archive.cloudera.com/cdh5/cdh/5/hbase-1.0.0-cdh5.4.9.tar.gz | tar xvz
cd hbase-1.0.0-cdh5.4.9
bin/start-hbase.sh
