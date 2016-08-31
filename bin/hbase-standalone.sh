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

# starts/stops/restarts an HBase server running in standalone mode.

usage="Usage: hbase-standalone.sh (start|stop|restart|run|status)"

bin=$(cd "$(dirname "${BASH_SOURCE-$0}")">/dev/null; pwd)

# load environment variables
. $bin/s2graph-env.sh
. $bin/s2graph-common.sh

# show usage when executed without enough arguments
if [ $# -lt 1 ]; then
  panic $usage
fi

S2GRAPH_OPTS="$S2GRAPH_OPTS -Dproc_master"
S2GRAPH_OPTS="$S2GRAPH_OPTS -Dhbase.log.dir=$S2GRAPH_LOG_DIR"
S2GRAPH_OPTS="$S2GRAPH_OPTS -Dhbase.log.file=hbase.logger.log"
S2GRAPH_OPTS="$S2GRAPH_OPTS -Dhbase.home.dir=$S2GRAPH_HOME"
S2GRAPH_OPTS="$S2GRAPH_OPTS -Dhbase.id.str=s2graph"
S2GRAPH_OPTS="$S2GRAPH_OPTS -Dhbase.root.logger=INFO,RFA"

export S2GRAPH_OPTS

$bin/s2graph-daemon.sh $1 hbase start
