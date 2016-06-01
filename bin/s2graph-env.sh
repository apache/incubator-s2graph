#! /usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This file sets appropriate environment variables for running S2Graph.
# Should be sourced and not be directly executed.
# The executable flag for this file should not be set, e.g. 644.

# Find the current directory
SOURCE="${BASH_SOURCE[0]}"
# resolve $SOURCE until the file is no longer a symlink
while [ -h "$SOURCE" ]; do
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"
done
DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

# set S2GRAPH_HOME
if [ -z $S2GRAPH_HOME ]; then
  export S2GRAPH_HOME="$( cd -P "$DIR/.." && pwd )"
fi

# set other environment variables for directories, relative to $S2GRAPH_HOME
# not intended to be individually configurable,
# configurations can be added or overridden in conf/application.conf
export S2GRAPH_BIN_DIR=$S2GRAPH_HOME/bin
export S2GRAPH_LIB_DIR=$S2GRAPH_HOME/lib
export S2GRAPH_LOG_DIR=$S2GRAPH_HOME/logs
export S2GRAPH_CONF_DIR=$S2GRAPH_HOME/conf
export S2GRAPH_HBASE_DIR=$S2GRAPH_HOME/var/hbase
export S2GRAPH_SQLITE_DIR=$S2GRAPH_HOME/var/sqlite
export S2GRAPH_PID_DIR=$S2GRAPH_HOME/var/pid
export S2GRAPH_ZOOKEEPER_DIR=$S2GRAPH_HOME/var/zookeeper

mkdir -p "$S2GRAPH_LOG_DIR" "$S2GRAPH_CONF_DIR" "$S2GRAPH_HBASE_DIR" \
    "$S2GRAPH_SQLITE_DIR" "$S2GRAPH_PID_DIR" "$S2GRAPH_ZOOKEEPER_DIR"

# apply the minimal JVM settings if none was given;
# in production, this variable is expected to be given by a deployment system.
if [ -z "$S2GRAPH_OPTS" ]; then
  export S2GRAPH_OPTS="-Xms1g -Xmx1g"
fi

# the maximum wait time for a process to finish, before sending SIGKILL
if [ -z "$S2GRAPH_STOP_TIMEOUT" ]; then
  export S2GRAPH_STOP_TIMEOUT=60 # in seconds
fi
