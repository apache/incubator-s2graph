#!/bin/bash
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

# Require $GREMLIN_HOME point to apache-tinkerpop-gremlin-console.

# remove pre-existing s2graph-gremlin plugin.
rm -rf ${GREMLIN_HOME}/ext/s2graph-gremlin

# remove pre-existing DB schema and HBase Storage.
rm -rf ${GREMLIN_HOME}/var ${GREMLIN_HOME}/storage

# export GREMLIN_HOME
# install plugin
${GREMLIN_HOME}/bin/gremlin.sh -e s2graph_install.groovy

