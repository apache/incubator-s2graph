#!/usr/bin/env sh

set -e

if [ ! -d $HOME/hbase/bin ]; then
  cd $HOME && wget -q -O - http://mirror.navercorp.com/apache/hbase/stable/hbase-$HBASE_VERSION-bin.tar.gz | tar xz
  ln -s $HOME/hbase-$HBASE_VERSION $HOME/hbase
fi
