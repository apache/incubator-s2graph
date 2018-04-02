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

package org.apache.s2graph.s2jobs.loader

import com.typesafe.config.Config
import org.apache.hadoop.hbase.{KeyValue => HKeyValue}
import org.apache.s2graph.core.GraphElement
import org.apache.s2graph.s2jobs.serde.Transformer
import org.apache.s2graph.s2jobs.serde.reader.{RowBulkFormatReader, TsvBulkFormatReader}
import org.apache.s2graph.s2jobs.serde.writer.KeyValueWriter
import org.apache.s2graph.s2jobs.{DegreeKey, S2GraphHelper}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

class SparkGraphElementLoaderTransformer(val config: Config,
                                         val options: GraphFileOptions) extends Transformer[Row, Seq[HKeyValue], org.apache.spark.rdd.RDD] {
  val reader = new RowBulkFormatReader

  val writer = new KeyValueWriter

  override def read(input: RDD[Row]): RDD[GraphElement] = input.mapPartitions { iter =>
    val s2 = S2GraphHelper.initS2Graph(config)

    iter.flatMap(reader.read(s2)(_))
  }

  override def write(elements: RDD[GraphElement]): RDD[Seq[HKeyValue]] = elements.mapPartitions { iter =>
    val s2 = S2GraphHelper.initS2Graph(config)

    iter.map(writer.write(s2)(_))
  }

  override def buildDegrees(elements: RDD[GraphElement]): RDD[Seq[HKeyValue]] = {
    val degrees = elements.mapPartitions { iter =>
      val s2 = S2GraphHelper.initS2Graph(config)

      iter.flatMap { element =>
        DegreeKey.fromGraphElement(s2, element, options.labelMapping).map(_ -> 1L)
      }
    }.reduceByKey(_ + _)

    degrees.mapPartitions { iter =>
      val s2 = S2GraphHelper.initS2Graph(config)

      iter.map { case (degreeKey, count) =>
        DegreeKey.toKeyValue(s2, degreeKey, count)
      }
    }
  }

  override def transform(input: RDD[Row]): RDD[Seq[HKeyValue]] = {
    val elements = read(input)
    val kvs = write(elements)

    if (options.buildDegree) kvs ++ buildDegrees(elements)
    kvs
  }
}
