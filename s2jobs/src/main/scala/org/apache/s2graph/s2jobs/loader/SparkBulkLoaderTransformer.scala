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
import org.apache.s2graph.core.GraphElement
import org.apache.s2graph.s2jobs.serde.{GraphElementReadable, GraphElementWritable, Transformer}
import org.apache.s2graph.s2jobs.{DegreeKey, S2GraphHelper}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class SparkBulkLoaderTransformer(val config: Config,
                                 val labelMapping: Map[String, String] = Map.empty,
                                 val buildDegree: Boolean = false) extends Transformer[RDD] {

  def this(config: Config, options: GraphFileOptions) =
    this(config, options.labelMapping, options.buildDegree)

  override def buildDegrees[T: ClassTag](elements: RDD[GraphElement])(implicit writer: GraphElementWritable[T]): RDD[T] = {
    val degrees = elements.mapPartitions { iter =>
      val s2 = S2GraphHelper.getS2Graph(config)

      iter.flatMap { element =>
        DegreeKey.fromGraphElement(s2, element, labelMapping).map(_ -> 1L)
      }
    }.reduceByKey(_ + _)

    degrees.mapPartitions { iter =>
      val s2 = S2GraphHelper.getS2Graph(config)

      iter.map { case (degreeKey, count) =>
        writer.writeDegree(s2)(degreeKey, count)
      }
    }
  }

  override def transform[S: ClassTag, T: ClassTag](input: RDD[S])(implicit reader: GraphElementReadable[S], writer: GraphElementWritable[T]): RDD[T] = {
    val elements = input.mapPartitions { iter =>
      val s2 = S2GraphHelper.getS2Graph(config)

      iter.flatMap { line =>
        reader.read(s2)(line)
      }
    }

    val kvs = elements.mapPartitions { iter =>
      val s2 = S2GraphHelper.getS2Graph(config)

      iter.map(writer.write(s2)(_))
    }

    if (buildDegree) kvs ++ buildDegrees(elements)
    else kvs
  }
}
