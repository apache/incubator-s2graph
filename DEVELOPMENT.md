<!---
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
--->

# S2Graph Development

## RAT

[Apache RAT](https://creadur.apache.org/rat/apache-rat/index.html) is an audits software tool.

To run it agains S2Graph source code, follow these steps:
    
    cd dev
    wget https://repo1.maven.org/maven2/org/apache/rat/apache-rat/0.12/apache-rat-0.12.jar
    ./run-rat.sh apache-rat-0.12.jar

The toll will write a report in case it finds any issue.
