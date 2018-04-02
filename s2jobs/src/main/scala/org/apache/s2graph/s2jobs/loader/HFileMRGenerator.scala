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

import java.util.UUID

import com.typesafe.config.Config
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.{HConstants, KeyValue}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.io.compress.Compression
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding
import org.apache.hadoop.hbase.mapreduce.{GraphHFileOutputFormat, HFileOutputFormat2}
import org.apache.hadoop.hbase.regionserver.BloomType
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, SequenceFileInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, SequenceFileOutputFormat}
import org.apache.hadoop.mapreduce.{Job, Mapper}
import org.apache.s2graph.s2jobs.serde.SparkBulkLoaderTransformer
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object HFileMRGenerator extends RawFileGenerator[String, KeyValue] {
  val DefaultBlockSize = 32768
  val DefaultConfig = Map(
    "yarn.app.mapreduce.am.resource.mb" -> 4096,
    "mapred.child.java.opts" -> "=-Xmx8g",
    "mapreduce.job.running.map.limit" -> 200,
    "mapreduce.job.running.reduce.limit" -> 200,
    "mapreduce.reduce.memory.mb" -> 8192,
    "mapreduce.reduce.java.opts" -> "-Xmx6144m",
    "mapreduce.map.memory.mb" -> "4096",
    "mapreduce.map.java.opts" -> "-Xmx3072m",
    "mapreduce.task.io.sort.factor" -> 10
  )

  class HFileMapper extends Mapper[ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, KeyValue] {
    override def map(key: ImmutableBytesWritable, value: ImmutableBytesWritable,
                     context: Mapper[ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, KeyValue]#Context): Unit = {
      val keyValue = new KeyValue(value.get(), 0, value.get().length)
      context.write(new ImmutableBytesWritable(keyValue.getRow), keyValue)
    }
  }

  def setMROptions(conf: Configuration) = {
    DefaultConfig.foreach { case (k, v) =>
      conf.set(k, conf.get(k, v.toString))
    }
  }

  def getStartKeys(numRegions: Int): Seq[ImmutableBytesWritable] = {
    HFileGenerator.getStartKeys(numRegions).map(new ImmutableBytesWritable(_))
  }

  def sortKeyValues(hbaseConf: Configuration,
                    tableName: String,
                    input: String,
                    output: String,
                    partitionSize: Int,
                    compressionAlgorithm: String) = {
    val job = Job.getInstance(hbaseConf, s"sort-MR $tableName")
    job.setJarByClass(getClass)
    job.setInputFormatClass(classOf[SequenceFileInputFormat[ImmutableBytesWritable, ImmutableBytesWritable]])
    job.setMapperClass(classOf[HFileMapper])
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[KeyValue])
    job.setOutputFormatClass(classOf[HFileOutputFormat2])
    job.getConfiguration.set(HConstants.TEMPORARY_FS_DIRECTORY_KEY, s"/tmp/${tableName}_staging")
    FileInputFormat.addInputPaths(job, input)
    FileOutputFormat.setOutputPath(job, new Path(output))


    import scala.collection.JavaConversions._

    GraphHFileOutputFormat.configureIncrementalLoad(job,
      getStartKeys(partitionSize),
      Seq("e", "v"),
      Compression.getCompressionAlgorithmByName(compressionAlgorithm.toLowerCase),
      BloomType.ROW,
      DefaultBlockSize,
      DataBlockEncoding.FAST_DIFF
    )
    job
  }

  def transfer(sc: SparkContext,
               s2Config: Config,
               input: RDD[String],
               options: GraphFileOptions): RDD[KeyValue] = {
    val transformer = new SparkBulkLoaderTransformer(s2Config, options)
    transformer.transform(input).flatMap(kvs => kvs)
  }

  override def generate(sc: SparkContext,
                        config: Config,
                        rdd: RDD[String],
                        options: GraphFileOptions) = {
    val merged = transfer(sc, config, rdd, options)
    val tmpOutput = options.tempDir

    merged.map(x => (new ImmutableBytesWritable(x.getKey), new ImmutableBytesWritable(x.getBuffer)))
      .saveAsNewAPIHadoopFile(tmpOutput,
        classOf[ImmutableBytesWritable],
        classOf[ImmutableBytesWritable],
        classOf[SequenceFileOutputFormat[ImmutableBytesWritable, ImmutableBytesWritable]])

    sc.killExecutors((1 to sc.defaultParallelism).map(_.toString))

    val conf = sc.hadoopConfiguration

    conf.setClassLoader(Thread.currentThread().getContextClassLoader())
    setMROptions(conf)

    val job = sortKeyValues(conf, options.tableName, tmpOutput, options.output,
      options.numRegions, options.compressionAlgorithm)

    val success = job.waitForCompletion(true)
    FileSystem.get(conf).delete(new Path(tmpOutput), true)
    if (!success) {
      throw new RuntimeException("mapreduce failed")
    }
  }

}
