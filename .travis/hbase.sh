#!/usr/bin/env bash

wget -q -O - http://mirror.navercorp.com/apache/hbase/stable/hbase-1.2.4-bin.tar.gz | tar xz
cd hbase-1.2.4
bin/start-hbase.sh
