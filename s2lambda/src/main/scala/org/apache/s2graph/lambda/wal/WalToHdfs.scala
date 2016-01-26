package org.apache.s2graph.lambda.wal

import java.text.SimpleDateFormat
import java.util.Date

import com.kakao.s2graph.core.Graph
import org.apache.s2graph.lambda.source.KafkaInput
import org.apache.s2graph.lambda._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

case class WalToHdfsParams(phase: String, dbUrl: String, brokerList: String,
    outputPath: String, splitListPath: String, hiveDatabase: Option[String], hiveTable: Option[String]) extends Params

class WalToHdfs(params: WalToHdfsParams) extends BaseDataProcessor[KafkaInput, EmptyData](params) {

  override protected def processBlock(input: KafkaInput, context: Context): EmptyData = {
    (params.hiveDatabase, params.hiveTable) match {
      case (Some(db), Some(tbl)) if context.sqlContext.isInstanceOf[HiveContext] =>
        logger.info("create the external table")
        context.sqlContext.sql(s"use $db")
        context.sqlContext.sql(
          s"""CREATE EXTERNAL TABLE IF NOT EXISTS `$tbl`(
              |  `log_ts` bigint,
              |  `operation` string,
              |  `log_type` string,
              |  `edge_from` string,
              |  `edge_to` string,
              |  `label` string,
              |  `props` string,
              |  `service` string)
              |PARTITIONED BY (
              |  `split` string,
              |  `date_id` string,
              |  `ts` string)
              |ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
              |STORED AS TEXTFILE
              |LOCATION '${params.outputPath}'
              """.stripMargin)
      case _ =>
    }

    val phase = params.phase
    val dbUrl = params.dbUrl
    val brokerList = params.brokerList
    val outputPath = params.outputPath
    val splitListPath = params.splitListPath
    val hiveDatabase = params.hiveDatabase
    val hiveTable = params.hiveTable

    val ts = input.time.milliseconds
    val sc = context.sparkContext

    try {
      val read = sc.textFile(splitListPath).collect().map(_.split("=")).flatMap {
        case Array(value) => Some(("split", value))
        case Array(key, value) => Some((key, value))
        case _ => None
      }
      WalToHdfs.splits = read.filter(_._1 == "split").map(_._2)
      WalToHdfs.excludeLabels = read.filter(_._1 == "exclude_label").map(_._2).toSet
      WalToHdfs.excludeServices = read.filter(_._1 == "exclude_service").map(_._2).toSet
    } catch {
      case _: Throwable => // use previous information
    }

    val elements = input.rdd.mapPartitions { partition =>
      // set executor setting.
      GraphSubscriberHelper.apply(phase, dbUrl, "none", brokerList)

      partition.flatMap { case (key, msg) =>
        val optMsg = Graph.toGraphElement(msg).flatMap { element =>
          val arr = msg.split("\t", 7)
          val service = element.serviceName
          val label = arr(5)
          val n = arr.length

          if (WalToHdfs.excludeServices.contains(service) || WalToHdfs.excludeLabels.contains(label)) {
            None
          } else if(n == 6) {
            Some(Seq(msg, "{}", service).mkString("\t"))
          }
          else if(n == 7) {
            Some(Seq(msg, service).mkString("\t"))
          }
          else {
            None
          }
        }
        optMsg
      }
    }

    val dateId = new SimpleDateFormat("yyyy-MM-dd").format(new Date(ts))

    /** make sure that `elements` are not running at the same time */
    val elementsWritten = {
      elements.cache()
      (Array("all") ++ WalToHdfs.splits).foreach {
        case split if split == "all" =>
          val path = s"$outputPath/split=$split/date_id=$dateId/ts=$ts"
          elements.saveAsTextFile(path)
        case split =>
          val path = s"$outputPath/split=$split/date_id=$dateId/ts=$ts"
          val strlen = split.length
          val splitData = elements.filter(_.takeRight(strlen) == split).cache()
          val totalSize = splitData
              .mapPartitions { iterator =>
                val s = iterator.map(_.length.toLong).sum
                Iterator.single(s)
              }
              .sum
              .toLong
          val numPartitions = math.max(1, (totalSize / WalToHdfs.hdfsBlockSize.toDouble).toInt)
          splitData.coalesce(math.min(splitData.partitions.length, numPartitions)).saveAsTextFile(path)
          splitData.unpersist()
      }
      elements.unpersist()
      elements
    }

    (hiveDatabase, hiveTable) match {
      case (Some(db), Some(tbl)) =>
        context.sqlContext match {
          case hiveContext: HiveContext =>
            (Array("all") ++ WalToHdfs.splits).foreach { split =>
              val path = s"$outputPath/split=$split/date_id=$dateId/ts=$ts"
              hiveContext.sql(s"use $db")
              hiveContext.sql(s"alter table $tbl add partition (split='$split', date_id='$dateId', ts='$ts') location '$path'")
            }
          case sqlContext: SQLContext =>
            logger.warn("hivemetastore seems not be allocated")
        }
      case _ =>
    }
    Data.emptyData
  }
}

object WalToHdfs {
  val hdfsBlockSize = 134217728 // 128M

  var splits = Array[String]()
  var excludeLabels = Set[String]()
  var excludeServices = Set[String]()
}
