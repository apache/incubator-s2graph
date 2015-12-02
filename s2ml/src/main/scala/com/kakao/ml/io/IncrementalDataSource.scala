package com.kakao.ml.io

import com.github.nscala_time.time.Imports._
import com.kakao.ml.{BaseDataProcessor, EmptyData, Params}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.json4s.JsonAST._
import org.json4s.native.JsonMethods

import scala.collection.mutable.ListBuffer

case class IncrementalDataSourceParams(
    service: String,
    duration: Int,
    labelWeight: Map[String, Double],
    baseRoot: String,
    incRoot: String,
    baseBefore: Option[Int],
    exceptIncrementalData: Option[Boolean],
    table: Option[String],
    outputField: Option[String],
    removeDuplications: Option[Boolean]) extends Params

class IncrementalDataSource(params: IncrementalDataSourceParams)
    extends BaseDataProcessor[EmptyData, SourceData](params) {

  import IncrementalDataSource._

  def getIncrementalPaths(fs: FileSystem, split: String, lastDateId: String): String = {

    val dateIds = new ListBuffer[String]

    var dtVar = DateTimeFormat.forPattern("yyyy-MM-dd").parseDateTime(lastDateId)
    val base = DateTime.now()

    while(dtVar < base) {
      dateIds.append(dtVar.toString("yyyy-MM-dd"))
      dtVar = dtVar.plusDays(1)
    }

    val subDirs = dateIds
        .map { dateId => new Path(params.incRoot + s"/split=$split/date_id=$dateId") }
        .filter(fs.exists)
        .toArray

    val paths = fs.listStatus(subDirs).map(_.getPath.toString)

    if(paths.length < 2) {
      paths.foreach(p => logInfo(p))
    } else {
      logInfo(s"${paths.length} paths")
      logInfo(paths.head)
      logInfo("...")
      logInfo(paths.last)
    }

    paths.mkString(",")
  }

  override def processBlock(sqlContext: SQLContext, input: EmptyData): SourceData = {

    import sqlContext.implicits._

    val sc = sqlContext.sparkContext
    val fs = FileSystem.get(sc.hadoopConfiguration)

    val removeDuplications = params.removeDuplications.getOrElse(false)
    val labelWeight = params.labelWeight
    val baseBefore = params.baseBefore.getOrElse(0)
    val split = if (fs.exists(new Path(params.baseRoot + s"/split=${params.service}"))) {
      params.service
    } else {
      logWarning("`split=all` is selected. make split for speed up")
      "all"
    }

    sc.parallelize(Seq("dual")).toDF("dual")
        .select(lit("date between (a, b), inclusive") as "explain",
          date_sub(current_date(), params.duration + baseBefore - 1) as "a",
          date_sub(current_date(), baseBefore) as "b")
        .show(false)

    val orcWithSchema = params.table match {
      case Some(table) =>
        sqlContext.table(table).where($"split" === split)
      case _ =>
        // currently, partition discovery does not seem stable.
        //    https://issues.apache.org/jira/browse/SPARK-10304
        // gives more explicit sub-directory as possible.
        sqlContext.read.schema(rawSchema).orc(params.baseRoot + s"/split=$split")
    }

    /** from daily data from baseRoot */
    val baseDF = orcWithSchema
        .where($"date_id".between(
          date_sub(current_date(), params.duration + baseBefore - 1),
          date_sub(current_date(), baseBefore)))
        .where($"label".isin(labelWeight.keys.toSeq: _*))
        .select("log_ts", "label", "edge_from", "edge_to", "props")

    val exceptIncrementalData = if(baseBefore != 0) {
      true
    } else {
      params.exceptIncrementalData.getOrElse(false)
    }

    var sourceDF = if(exceptIncrementalData) {
      baseDF
    } else {
      /** from incremental data */
      val lastDateId = fs.listStatus(new Path(s"${params.baseRoot}/split=$split"))
          .map(_.getPath.toString)
          .filter(_.contains("date_id=")) match {
        case x if x.nonEmpty => x.max.split("date_id=")(1)
        case _ => DateTime.now().minusDays(params.duration).toString("yyyy-MM-dd")
      }

      val bcLabelMap = sc.broadcast(labelWeight.keys.toSet)

      val incrementalDF = sc.textFile(getIncrementalPaths(fs, split, lastDateId)).map(_.split("\t", -1))
          .flatMap {
            case Array(log_ts, operation, log_type, edge_from, edge_to, l, props, s) if bcLabelMap.value.contains(l) =>
              Some((log_ts.toLong, l, edge_from, edge_to, props))
            case _ =>
              None
          }
          .toDF("log_ts", "label", "edge_from", "edge_to", "props")

      baseDF.unionAll(incrementalDF)
    }

    if (removeDuplications) {
      sourceDF = sourceDF.distinct()
    }

    // apply `outputField`
    if (params.outputField.isDefined) {
      val outputField = params.outputField.get
      sourceDF = sourceDF
          .drop("edge_to")
          .withColumn("edge_to", getJsonObject($"props", lit(outputField)))
    }

    sourceDF = sourceDF.select($"log_ts" as tsColString, $"label"
        as labelColString, $"edge_from" as userColString, $"edge_to" as itemColString)

    /** materialize */
    sourceDF.persist(StorageLevel.MEMORY_AND_DISK)
    val (tsCount, tsFrom, tsTo) = sourceDF.select(count(tsCol), min(tsCol), max(tsCol)).head(1).map { row =>
      (row.getLong(0), row.getLong(1), row.getLong(2))
    }.head

    SourceData(sourceDF, tsCount, tsFrom, tsTo, labelWeight, None, None)
  }

}

object IncrementalDataSource {

  val getJsonObject = udf { (props: String, outputField: String) =>
    val v = JsonMethods.parse(props) \ outputField match {
      case JNothing => null
      case JNull => null
      case JDouble(x) => x
      case JDecimal(x) => x
      case JInt(x) => x
      case JBool(x) => x
      case _ => null
    }
    // Schema for type Any is not supported
    v.toString
  }

}
