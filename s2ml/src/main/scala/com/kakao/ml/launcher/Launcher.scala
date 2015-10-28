package com.kakao.ml.launcher

import java.util.UUID

import com.google.common.io.BaseEncoding
import com.kakao.ml.util.Json
import com.kakao.ml.{BaseDataProcessor, Data, Params}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkConf, SparkContext}

import scala.reflect.ClassTag

case class ProcessorFactory(className: String, paramsAsMap: Map[String, Any]) {

  private val constructor = Class.forName(className).getConstructors.head

  def getInstance: BaseDataProcessor[Data, Data] = {
    val instance = constructor.getParameterTypes match {
      case Array() =>
        constructor.newInstance()
      case Array(pClass) if classOf[Params].isAssignableFrom(pClass) =>
        val params = Json.extract(paramsAsMap)(ClassTag(pClass)).asInstanceOf[Params]
        constructor.newInstance(params)
      case _ =>
        require(false, Seq(constructor.getParameterTypes.toSeq, className, paramsAsMap).map(_.toString).mkString(","))
    }
    instance.asInstanceOf[BaseDataProcessor[Data, Data]]
  }

}

case class ProcessorDesc(
    `class`: String,
    params: Option[Map[String, Any]],
    id: Option[String],
    pid: Option[String],
    pids: Option[Seq[String]])

case class JobDesc(name: String, env: Option[Map[String, Any]], processors: List[ProcessorDesc])

object Launcher extends Environment with Visualization with Logging {

  val sparkUser = Option(sys.props("user.name")).getOrElse("unknown")
  val className: String = getClass.getName.stripSuffix("$")
  val name: String = className + "@" + sparkUser

  def main(args: Array[String]): Unit = {
    logInfo(s"Running $name")

    require(args.length >= 2)

    val Array(command, encoded) = args.slice(0, 2)

    val jsonString = new String(BaseEncoding.base64().decode(encoded), "UTF-8")

    launch(jsonString, command, None)
  }

  def launch(jsonString: String, command: String, sparkContext: Option[SparkContext] = None): Unit = {

    val jobDesc = Json.extract[JobDesc](jsonString)

    setJobId(jobDesc.name)
    jobDesc.env.foreach(setEnv)

    logInfo(Json.toPrettyJsonString(jobDesc))

    val instances = jobDesc.processors.zipWithIndex.map { case (p, order) =>
      val id = p.id.getOrElse(UUID.randomUUID().toString)
      val pids = (p.pid, p.pids) match {
        case (None, None) => Seq.empty[String]
        case (Some(a), None) => Seq(a)
        case (None, Some(b)) => b
        case (Some(a), Some(b)) => Seq(a) ++ b
      }
      val instance = ProcessorFactory(p.`class`, p.params.getOrElse(Map.empty[String, Any])).getInstance
      (order, id, pids, instance)
    }

    val keyByOrder = instances.map(x => x._1 -> x).toMap
    val keyById = instances.map(x => x._2 -> x).toMap

    val rootId = "root"

    instances.foreach { case (order, id, pids, instance) =>
      val predecessors = pids match {
        case s if s.isEmpty && order == 0 => Seq.empty[BaseDataProcessor[_, _]]
        case s if s.isEmpty => Seq(keyByOrder(order - 1)._4)
        case seq if seq.length == 1 && seq.contains(rootId) => Seq()
        case seq => seq.filter(_ != rootId).map(keyById).map(_._4)
      }

      val depth = predecessors.map(_.getDepth) match {
        case x if x.nonEmpty => x.max + 1
        case _ => 0
      }

      instance
          .setOrder(order)
          .setDepth(depth)
          .setPredecessors(predecessors: _*)
    }

    val edges = instances.flatMap { case (order, id, pids, instance) =>
      if (instance.getPredecessors.isEmpty) {
        Seq("root" -> instance.toString(false))
      } else {
        instance.getPredecessors.map(_.toString(false) -> instance.toString(false))
      }
    }

    logInfo("********** The Overall Execution Plan **********")
    logInfo(getExecutionPlan(edges))

    val leaves = instances.map(_._4).diff(instances.flatMap(_._4.getPredecessors))

    logInfo("********** Leaves **********")
    leaves.foreach(x => logInfo(x.toString(false)))

    command match {
      case "show" => // show plan only
      case "run" =>
        val sqlContext = if(sparkContext.isDefined) {
          new HiveContext(sparkContext.get)
        } else {
          val sparkConf = new SparkConf().setAppName(name + s"#$getJobId-$getBatchId")
          val sc = new SparkContext(sparkConf)
          new HiveContext(sc)
        }
        leaves.foreach(_.process(sqlContext))
    }

  }

}
