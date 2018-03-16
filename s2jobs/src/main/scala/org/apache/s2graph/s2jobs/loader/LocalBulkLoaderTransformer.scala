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
import org.apache.hadoop.hbase.KeyValue
import org.apache.s2graph.core.{GraphElement, S2Graph}
import org.apache.s2graph.s2jobs.serde.Transformer
import org.apache.s2graph.s2jobs.serde.reader.TsvBulkFormatReader
import org.apache.s2graph.s2jobs.serde.writer.KeyValueWriter
import org.apache.s2graph.s2jobs.{DegreeKey, S2GraphHelper}

import scala.concurrent.ExecutionContext

class LocalBulkLoaderTransformer(val config: Config,
                                 val options: GraphFileOptions)(implicit ec: ExecutionContext) extends Transformer[String, Seq[KeyValue], Seq] {
  val s2: S2Graph = S2GraphHelper.initS2Graph(config)

  override val reader = new TsvBulkFormatReader
  override val writer = new KeyValueWriter

  override def read(input: Seq[String]): Seq[GraphElement] = input.flatMap(reader.read(s2)(_))

  override def write(elements: Seq[GraphElement]): Seq[Seq[KeyValue]] = elements.map(writer.write(s2)(_))

  override def buildDegrees(elements: Seq[GraphElement]): Seq[Seq[KeyValue]] = {
    val degrees = elements.flatMap { element =>
      DegreeKey.fromGraphElement(s2, element, options.labelMapping).map(_ -> 1L)
    }.groupBy(_._1).mapValues(_.map(_._2).sum)

    degrees.toSeq.map { case (degreeKey, count) =>
      DegreeKey.toKeyValue(s2, degreeKey, count)
    }
  }

  override def transform(input: Seq[String]): Seq[Seq[KeyValue]] = {
    val elements = read(input)
    val kvs = write(elements)

    val degrees = if (options.buildDegree) buildDegrees(elements) else Nil

    kvs ++ degrees
  }
}
