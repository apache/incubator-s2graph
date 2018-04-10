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

# starts/stops/restarts a java application as a daemon process
# also able to run the application directly using run

usage="This script is intended to be used by other scripts in this directory."
usage=$"$usage\n Please refer to start-s2graph.sh and stop-s2graph.sh"
usage=$"$usage\n Usage: s2graph-daemon.sh (start|stop|restart|run|status)"
usage="$usage (s2rest_play|s2rest_netty|s2graphql|...) <args...>"

bin=$(cd "$(dirname "${BASH_SOURCE-$0}")">/dev/null; pwd)

# load environment variables
. $bin/s2graph-env.sh
. $bin/s2graph-common.sh

# show usage when executed without enough arguments
if [ $# -le 1 ]; then
  panic "$usage"
fi

# to enable the JVM application to use relative paths as well
cd "$S2GRAPH_HOME"

# arguments
action=$1
shift
service=$1
shift
this="$bin/$(basename ${BASH_SOURCE-$0})"
args="$@"

# locate java
if [ -z $JAVA_HOME ]; then
  JAVA=$(which java)
else
  JAVA=$JAVA_HOME/bin/java
fi

# require JVM 8
JAVA_VERSION=$($JAVA -version 2>&1 | sed 's/.*version "\(.*\)\.\(.*\)\..*"/\1\2/; 1q')
if [ $JAVA_VERSION -lt 18 ]; then
  panic "Java 8 or later is required"
fi

# construct the classpath
classpath="$S2GRAPH_CONF_DIR:$S2GRAPH_LIB_DIR/*"

# determine the main class
case $service in
s2rest_play)
  main="play.core.server.NettyServer"
  ;;
s2graphql)
  main="org.apache.s2graph.graphql.Server"
  ;;
hbase)
  main="org.apache.hadoop.hbase.master.HMaster"
  ;;
h2)
  main="org.h2.tools.Server"
  ;;
*)
  panic "Unknown service: $service"
esac

# file that contains the service's pid
pidfile="$S2GRAPH_PID_DIR/$service.pid"

# file that contains the console output of the JVM
# this file should only contain abnormal logs, while
# the normal logs should go to the log files as configured separately,
# ideally with daily log-rotation.
log="$S2GRAPH_LOG_DIR/$service.log"

start() {
  if [ -f "$pidfile" ]; then
    if kill -0 $(cat "$pidfile") > /dev/null 2>&1; then
      panic "$service is already running as process $(cat "$pidfile")"
    fi
    rm -r "$pidfile"
  fi
  echo "Starting $service..." 1>&2

  "$this" run "$service" $args < /dev/null >> "$log" 2>&1 &
  disown -h -r

  # sleep enough, to check if there was any error during startup
  sleep 3
  if [ -f "$pidfile" ]; then
    echo "Service $service is running as process $(cat "$pidfile")"
  else
    echo "Service $service might have failed to start; see $log"
  fi
}

stop() {
  if [ -f "$pidfile" ]; then
    if kill -0 $(cat "$pidfile") > /dev/null 2>&1; then
      echo "Stopping $service" 1>&2
      terminate $(cat "$pidfile")
    else
      echo "Process $(cat "$pidfile") is not running" 1>&2
    fi
  else
    echo "PID file does not exist at $pidfile" 1>&2
  fi
  rm -f "$pidfile"
}

cleanup() {
  if [ -f "$pidfile" ]; then
    if kill -0 $(cat "$pidfile") > /dev/null 2>&1; then
      terminate $(cat "$pidfile")
    fi
    rm -f "$pidfile"
  fi
}

status() {
  if [ -f "$pidfile" ]; then
    if kill -0 $(cat "$pidfile") > /dev/null 2>&1; then
      echo "Service $service is running as process $(cat "$pidfile")" 1>&2
    else
      echo "PID file for service $service exists at $pidfile, " \
       "but process $(cat "$pidfile") is not running" 1>&2
    fi
  else
    echo "Service $service is not running" 1>&2
  fi
}

run() {
  trap cleanup SIGHUP SIGINT SIGTERM EXIT
  echo "$JAVA" -cp "$classpath" -Ds2graph.home="$S2GRAPH_HOME" $S2GRAPH_OPTS "$main" $args 2>&1 &
  "$JAVA" -cp "$classpath" -Ds2graph.home="$S2GRAPH_HOME" $S2GRAPH_OPTS "$main" $args 2>&1 &
  pid=$!
  echo "$pid" > "$pidfile"
  wait "$pid"
}

case $action in
start)
  start
  ;;
stop)
  stop
  ;;
restart)
  stop
  start
  ;;
status)
  status
  ;;
run)
  run
  ;;
*)
  panic "$usage"
esac
