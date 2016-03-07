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

package s2.spark

import kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.{KafkaUtils, StreamHelper}
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{Accumulable, Logging, SparkConf}

import scala.collection.mutable.{HashMap => MutableHashMap}


/**
 * Created by hsleep(honeysleep@gmail.com) on 14. 12. 26..
 */
trait SparkApp extends Logging {
  type HashMapAccumulable = Accumulable[MutableHashMap[String, Long], (String, Long)]

  protected def args: Array[String] = _args

  private var _args: Array[String] = _

  private var streamHelper: StreamHelper = _

  // should implement in derived class
  def run()

  def getArgs(index: Int) = args(index)

  def main(args: Array[String]) {
    _args = args
    run()
  }

  def validateArgument(argNames: String*): Unit = {
    if (args == null || args.length < argNames.length) {
      System.err.println(s"Usage: ${getClass.getName} " + argNames.map(s => s"<$s>").mkString(" "))
      System.exit(1)
    }
  }

  def buildKafkaGroupId(topic: String, ext: String): String = {
    val phase = System.getProperty("phase")

    // use first topic for group id
    val groupId = s"${topic.split(',')(0)}_$ext"

    groupId + {
      phase match {
        case "real" | "production" => ""
        case x => s"_$x"
      }
    }
  }

  def getStreamHelper(kafkaParam: Map[String, String]): StreamHelper = {
    if (streamHelper == null) {
      this.synchronized {
        if (streamHelper == null) {
          streamHelper = StreamHelper(kafkaParam)
        }
      }
    }
    streamHelper
  }

  def sparkConf(jobName: String): SparkConf = {
    val conf = new SparkConf()
    conf.setAppName(jobName)
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.streaming.unpersist", "true")
    conf
  }

  def streamingContext(sparkConf: SparkConf, interval: Duration, checkPoint: Option[String] = None) = {
    val ssc = new StreamingContext(sparkConf, interval)
    checkPoint.foreach { dir =>
      ssc.checkpoint(dir)
    }

    // for watch tower
    ssc.addStreamingListener(new SubscriberListener(ssc))

    ssc
  }

  def createKafkaPairStream(ssc: StreamingContext, kafkaParam: Map[String, String], topics: String, numPartition: Option[Int] = None): DStream[(String, String)] = {
    val topicMap = topics.split(",").map((_, 1)).toMap
    val stream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParam, topicMap, StorageLevel.MEMORY_AND_DISK_SER_2)
    numPartition.map(n =>
      stream.repartition(n)
    ).getOrElse(stream)
  }

  def createKafkaValueStream(ssc: StreamingContext, kafkaParam: Map[String, String], topics: String, numPartition: Option[Int] = None): DStream[String] = {
    createKafkaPairStream(ssc, kafkaParam, topics, numPartition).map(_._2)
  }

  def createKafkaPairStreamMulti(ssc: StreamingContext, kafkaParam: Map[String, String], topics: String, receiverCount: Int, numPartition: Option[Int] = None): DStream[(String, String)] = {
    // wait until all executor is running
    Stream.continually(ssc.sparkContext.getExecutorStorageStatus).takeWhile(_.length < receiverCount).foreach { arr =>
      Thread.sleep(100)
    }
    Thread.sleep(1000)

    val topicMap = topics.split(",").map((_, 1)).toMap

    val stream = {
      val streams = {
        (1 to receiverCount) map { _ =>
          KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParam, topicMap, StorageLevel.MEMORY_AND_DISK_SER_2)
        }
      }
      ssc.union(streams)
    }
    numPartition.map(n =>
      stream.repartition(n)
    ).getOrElse(stream)
  }

  def createKafkaValueStreamMulti(ssc: StreamingContext, kafkaParam: Map[String, String], topics: String, receiverCount: Int, numPartition: Option[Int] = None): DStream[String] = {
    createKafkaPairStreamMulti(ssc, kafkaParam, topics, receiverCount, numPartition).map(_._2)
  }
}
