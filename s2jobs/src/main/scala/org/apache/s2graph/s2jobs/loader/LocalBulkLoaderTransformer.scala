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
import org.apache.s2graph.core.{GraphElement, S2Graph}
import org.apache.s2graph.s2jobs.serde.{GraphElementReadable, GraphElementWritable, Transformer}
import org.apache.s2graph.s2jobs.{DegreeKey, S2GraphHelper}

import scala.concurrent.ExecutionContext
import scala.reflect.ClassTag

class LocalBulkLoaderTransformer(val config: Config,
                                 val options: GraphFileOptions)(implicit ec: ExecutionContext) extends Transformer[Seq] {
  val s2: S2Graph = S2GraphHelper.getS2Graph(config)

  override def buildDegrees[T: ClassTag](elements: Seq[GraphElement])(implicit writer: GraphElementWritable[T]): Seq[T] = {
    val degrees = elements.flatMap { element =>
      DegreeKey.fromGraphElement(s2, element, options.labelMapping).map(_ -> 1L)
    }.groupBy(_._1).mapValues(_.map(_._2).sum)

    degrees.toSeq.map { case (degreeKey, count) =>
      writer.writeDegree(s2)(degreeKey, count)
    }
  }

  override def transform[S: ClassTag, T: ClassTag](input: Seq[S])(implicit reader: GraphElementReadable[S], writer: GraphElementWritable[T]): Seq[T] = {
    val elements = input.flatMap(reader.read(s2)(_))
    val kvs = elements.map(writer.write(s2)(_))
    val degrees = if (options.buildDegree) buildDegrees[T](elements) else Nil

    kvs ++ degrees
  }
}
