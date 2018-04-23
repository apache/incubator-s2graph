#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
source common.sh

[ $# -ne 1 ] && { usage; }

SERVICE=$1
JOBDESC_TPL=${WORKDIR}/${SERVICE}/jobdesc.template
JOBDESC=${WORKDIR}/${SERVICE}/jobdesc.json
WORKING_DIR=`pwd`
JAR=`ls ${ROOTDIR}/s2jobs/target/scala-2.11/s2jobs-assembly*.jar`
LIB=`cd ${ROOTDIR}/target/apache-s2graph-*-incubating-bin/lib; pwd`

info "WORKING_DIR : $WORKING_DIR"
info "JAR : $JAR"
info "LIB : $LIB"

sed -e "s/\[=WORKING_DIR\]/${WORKING_DIR//\//\\/}/g" $JOBDESC_TPL | grep "^[^#]" > $JOBDESC

unset HADOOP_CONF_DIR
info "spark submit.."
${SPARK_HOME}/bin/spark-submit \
  --class org.apache.s2graph.s2jobs.JobLauncher \
  --master local[2] \
  --jars $LIB/h2-1.4.192.jar,$LIB/lucene-core-7.1.0.jar \
  --driver-class-path "$LIB/h2-1.4.192.jar:$LIB/lucene-core-7.1.0.jar" \
  --driver-java-options "-Dderby.system.home=/tmp/derby" \
  $JAR file -f $JOBDESC -n SAMPLEJOB:$SERVICE 

