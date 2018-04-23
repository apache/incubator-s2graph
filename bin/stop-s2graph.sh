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


# Stops the S2Graph server, along with an hbase server as required

bin=$(cd "$(dirname "${BASH_SOURCE-$0}")">/dev/null; pwd)

# load environment variables
. $bin/s2graph-env.sh
. $bin/s2graph-common.sh

service="s2rest_play"
[ $# -gt 0 ] && { service=$1; }
$bin/s2graph.sh stop ${service}
$bin/s2graph-daemon.sh stop h2
$bin/hbase-standalone.sh stop
