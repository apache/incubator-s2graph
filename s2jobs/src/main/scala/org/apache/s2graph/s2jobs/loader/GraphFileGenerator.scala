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

import org.apache.s2graph.core._
import org.apache.spark.{SparkConf, SparkContext}

object GraphFileGenerator {
  def main(args: Array[String]): Unit = {
    val options = GraphFileOptions.toOption(args)

    val s2Config = Management.toConfig(options.toConfigParams)

    val conf = new SparkConf()
    conf.setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)

    val input = sc.textFile(options.input)
    options.method match {
      case "MR" => HFileMRGenerator.generate(sc, s2Config, input, options)
      case "SPARK" => HFileGenerator.generate(sc, s2Config, input, options)
      case _ => throw new IllegalArgumentException("only supported type is MR/SPARK.")
    }
    sc.stop()
  }
}
