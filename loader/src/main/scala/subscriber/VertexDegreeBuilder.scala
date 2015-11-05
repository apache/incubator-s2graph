package subscriber

/**
 * Created by shon on 7/3/15.
 */

import com.kakao.s2graph.core.{GraphUtil, Management}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import s2.spark.{HashMapParam, WithKafka, SparkApp}

import scala.collection.mutable

/**
 * Created by shon on 7/3/15.
 */

//object VertexDegreeBuilder extends SparkApp with WithKafka {
//  val sleepPeriod = 5000
//  val usages =
//    s"""
//       |/**
//       |* this job read edge format(TSV) from HDFS file system then bulk load edges into s2graph. assumes that newLabelName is already created by API.
//       |* params:
//       |*  0. hdfsPath: where is your data in hdfs. require full path with hdfs:// predix
//       |*  1. dbUrl: jdbc database connection string to specify database for meta.
//       |*  2. labelMapping: oldLabel:newLabel delimited by ,
//       |*  3. outputPath: degree output Path.
//       |*  4. edgeAutoCreate: true if need to create reversed edge automatically.
//       |*
//       |* after this job finished, s2graph will have data with sequence corresponding newLabelName.
//       |* change this newLabelName to ogirinalName if you want to online replace of label.
//       |*
//       |*/
//     """.stripMargin
//
//  override def run() = {
//    /**
//     * Main function
//     */
//    println(args.toList)
//    if (args.length != 5) {
//      System.err.println(usages)
//      System.exit(1)
//    }
//    val hdfsPath = args(0)
//    val dbUrl = args(1)
//    val labelMapping = GraphSubscriberHelper.toLabelMapping(args(2))
//    val outputPath = args(3)
//    val edgeAutoCreate = args(4).toBoolean
//
//    val conf = sparkConf(s"$hdfsPath: VertexDegreeBuilder")
//    val sc = new SparkContext(conf)
//    val mapAcc = sc.accumulable(mutable.HashMap.empty[String, Long], "counter")(HashMapParam[String, Long](_ + _))
//
//    /** this job expect only one hTableName. all labels in this job will be stored in same physical hbase table */
//    try {
//
//      // set local driver setting.
//      val phase = System.getProperty("phase")
//      GraphSubscriberHelper.apply(phase, dbUrl, "none", "none")
//
//      /** copy when oldLabel exist and newLabel done exist. otherwise ignore. */
//
//
//      val msgs = sc.textFile(hdfsPath)
//
//      /** change assumption here. this job only take care of one label data */
//      val degreeStart: RDD[((String, String, String), Int)] = msgs.filter { msg =>
//        val tokens = GraphUtil.split(msg)
//        (tokens(2) == "e" || tokens(2) == "edge")
//      } flatMap { msg =>
//        val tokens = GraphUtil.split(msg)
//        val tempDirection = if (tokens.length == 7) "out" else tokens(7)
//        val direction = if (tempDirection != "out" && tempDirection != "in") "out" else tempDirection
//        val reverseDirection = if (direction == "out") "in" else "out"
//        val convertedLabelName = labelMapping.get(tokens(5)).getOrElse(tokens(5))
//        val (vertexWithLabel, reverseVertexWithLabel) = if (direction == "out") {
//          (
//            (tokens(3), convertedLabelName, direction),
//            (tokens(4), convertedLabelName, reverseDirection)
//            )
//        } else {
//          (
//            (tokens(4), convertedLabelName, direction),
//            (tokens(3), convertedLabelName, reverseDirection)
//            )
//        }
//        if (edgeAutoCreate) {
//          List((vertexWithLabel -> 1), (reverseVertexWithLabel -> 1))
//        } else {
//          List((vertexWithLabel -> 1))
//        }
//      }
//      val vertexDegrees = degreeStart.reduceByKey(_ + _)
//      vertexDegrees.map { case ((vertexId, labelName, dir), degreeVal) =>
//        Seq(vertexId, labelName, dir, degreeVal).mkString("\t")
//      }.saveAsTextFile(outputPath)
//
//    }
//  }
//}
