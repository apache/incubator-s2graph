#!/usr/bin/env sh

set -xe

if [ ! -d "$HOME/hbase-$HBASE_VERSION/bin" ]; then
  cd $HOME && wget -q -O - http://mirror.navercorp.com/apache/hbase/stable/hbase-$HBASE_VERSION-bin.tar.gz | tar xz
fi
