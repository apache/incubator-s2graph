#!/usr/bin/env bash
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

# print an error message to STDERR and quit
panic() {
  echo "$@" 1>&2
  exit -1
}

# stop a process with specified pid, first by SIGTERM and then by SIGKILL
terminate() {
  for pid in "$@"; do
    started=$(date +%s)
    kill $pid > /dev/null 2>&1
    printf "Waiting process $pid to finish" 1>&2
    while kill -0 $pid > /dev/null 2>&1; do
      sleep 0.5
      printf "." 1>&2
      if [ $(( $(date +%s) - $started )) -ge $S2GRAPH_STOP_TIMEOUT ]; then
        break
      fi
    done
    printf "\n" 1>&2
    if kill -0 $pid > /dev/null 2>&1; then
      echo "Force stopping $pid with a SIGKILL" 1>&2
      kill -9 $pid > /dev/null 2>&1
    fi
  done
}
