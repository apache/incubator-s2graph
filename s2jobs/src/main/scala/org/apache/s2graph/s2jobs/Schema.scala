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

package org.apache.s2graph.s2jobs

import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object Schema {
  /**
    * root
    * |-- timestamp: long (nullable = false)
    * |-- operation: string (nullable = false)
    * |-- elem: string (nullable = false)
    */
  val CommonFields = Seq(
    StructField("timestamp", LongType, nullable = false),
    StructField("operation", StringType, nullable = false),
    StructField("elem", StringType, nullable = false)
  )

  val BulkLoadSchema = StructType(CommonFields ++ Seq(
    StructField("from", StringType, false),
    StructField("to", StringType, false),
    StructField("label", StringType, false),
    StructField("props", StringType, false),
    StructField("direction", StringType, true)
  ))

  /**
    * root
    * |-- timestamp: long (nullable = true)
    * |-- operation: string (nullable = true)
    * |-- elem: string (nullable = true)
    * |-- id: string (nullable = true)
    * |-- service: string (nullable = true)
    * |-- column: string (nullable = true)
    * |-- props: string (nullable = true)
    */
  val VertexSchema = StructType(CommonFields ++ Seq(
    StructField("id", StringType, false),
    StructField("service", StringType, false),
    StructField("column", StringType, false),
    StructField("props", StringType, false)
  ))


  /**
    * root
    * |-- timestamp: long (nullable = true)
    * |-- operation: string (nullable = true)
    * |-- elem: string (nullable = true)
    * |-- from: string (nullable = true)
    * |-- to: string (nullable = true)
    * |-- label: string (nullable = true)
    * |-- props: string (nullable = true)
    * |-- direction: string (nullable = true)
    */
  val EdgeSchema = StructType(CommonFields ++ Seq(
    StructField("from", StringType, false),
    StructField("to", StringType, false),
    StructField("label", StringType, false),
    StructField("props", StringType, false),
    StructField("direction", StringType, true)
  ))

  /**
    * root
    * |-- timestamp: long (nullable = false)
    * |-- operation: string (nullable = false)
    * |-- elem: string (nullable = false)
    * |-- id: string (nullable = true)
    * |-- service: string (nullable = true)
    * |-- column: string (nullable = true)
    * |-- from: string (nullable = true)
    * |-- to: string (nullable = true)
    * |-- label: string (nullable = true)
    * |-- props: string (nullable = true)
    */
  val GraphElementSchema = StructType(CommonFields ++ Seq(
    StructField("id", StringType, nullable = true),
    StructField("service", StringType, nullable = true),
    StructField("column", StringType, nullable = true),
    StructField("from", StringType, nullable = true),
    StructField("to", StringType, nullable = true),
    StructField("label", StringType, nullable = true),
    StructField("props", StringType, nullable = true)
  ))
}
