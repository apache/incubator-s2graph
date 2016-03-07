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

package org.apache.spark.streaming.kafka

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD

import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 5. 6..
 */
class KafkaRDDFunctions[T: ClassTag](self: RDD[T])
  extends Logging
  with Serializable
{
  def foreachPartitionWithOffsetRange(f: (OffsetRange, Iterator[T]) => Unit): Unit = {
    val offsets = self.asInstanceOf[HasOffsetRanges].offsetRanges
    foreachPartitionWithIndex { (i, part) =>
      val osr: OffsetRange = offsets(i)
      f(osr, part)
    }
  }

  def foreachPartitionWithIndex(f: (Int, Iterator[T]) => Unit): Unit = {
    self.mapPartitionsWithIndex[Nothing] { (i, part) =>
      f(i, part)
      Iterator.empty
    }.foreach {
      (_: Nothing) => ()
    }
  }
}

object KafkaRDDFunctions {
  implicit def rddToKafkaRDDFunctions[T: ClassTag](rdd: RDD[T]): KafkaRDDFunctions[T] = {
    new KafkaRDDFunctions(rdd)
  }
}
