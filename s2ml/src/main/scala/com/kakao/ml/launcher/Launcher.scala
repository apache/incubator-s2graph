package com.kakao.ml.launcher

import java.util.UUID

import com.kakao.ml.util.Json
import com.kakao.ml.{BaseDataProcessor, Data, Params}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag

case class ProcessorFactory(className: String, paramsAsJValue: Json.JsonValue) {

  private val constructor = Class.forName(className).getConstructors.head

  def getInstance: BaseDataProcessor[Data, Data] = {
    val instance = constructor.getParameterTypes match {
      case Array() =>
        constructor.newInstance()
      case Array(pClass) if classOf[Params].isAssignableFrom(pClass) =>
        val params = Json.extract(paramsAsJValue)(ClassTag(pClass)).asInstanceOf[Params]
        constructor.newInstance(params)
      case _ =>
        require(false,
          Seq(constructor.getParameterTypes.toSeq, className, paramsAsJValue).map(_.toString).mkString(","))
    }
    instance.asInstanceOf[BaseDataProcessor[Data, Data]]
  }

}

case class ProcessorDesc(
    `class`: String,
    params: Option[Json.JsonValue],
    id: Option[String],
    pid: Option[String],
    pids: Option[Seq[String]])

case class JobDesc(
    name: String,
    processors: List[ProcessorDesc],
    root: Option[String],
    env: Option[Map[String, Any]],
    comment: Option[String])

object Launcher extends Environment with Visualization {

  val sparkUser = Option(sys.props("user.name")).getOrElse("unknown")
  val className: String = getClass.getName.stripSuffix("$")
  val name: String = className + "@" + sparkUser

  def main(args: Array[String]): Unit = {
    println(s"Running $name")

    require(args.length >= 2)

    val Array(command, encoded) = args.slice(0, 2)

    val jsonString = new String(javax.xml.bind.DatatypeConverter.parseBase64Binary(encoded), "UTF-8")

    launch(jsonString, command, None)
  }

  def buildPipeline(jsonString: String): List[BaseDataProcessor[Data, Data]]  = {
    
    val jobDesc = Json.extract[JobDesc](jsonString)

    /** set env params */
    setJobId(jobDesc.name)
    jobDesc.root.foreach(setRootDir)
    jobDesc.env.foreach(setEnv)
    jobDesc.comment.foreach(setComment)

    /** show jobDesc */
    println(Json.toPrettyJsonString(jobDesc))

    /** instantiation */
    val instances = jobDesc.processors.zipWithIndex.map { case (p, order) =>
      val id = p.id.getOrElse(UUID.randomUUID().toString)
      val pids = (p.pid, p.pids) match {
        case (None, None) => Seq.empty[String]
        case (Some(a), None) => Seq(a)
        case (None, Some(b)) => b
        case (Some(a), Some(b)) => Seq(a) ++ b
      }
      val instance = ProcessorFactory(p.`class`, p.params.getOrElse(Json.emptyJsonValue)).getInstance
      (order, id, pids, instance)
    }

    /** add dependencies for each processor */
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

    /** get dependency edges for visualization */
    val edges = instances.flatMap { case (order, id, pids, instance) =>
      if (instance.getPredecessors.isEmpty) {
        Seq("root" -> instance.toString(false))
      } else {
        instance.getPredecessors.map(_.toString(false) -> instance.toString(false))
      }
    }

    println("********** The Overall Execution Plan **********")
    println(getExecutionPlan(edges))

    println("********** Leaves **********")
    val leaves = instances.map(_._4).diff(instances.flatMap(_._4.getPredecessors))
    leaves.foreach(x => println(x.toString(false)))

    leaves

  }

  def launch(jsonString: String, command: String, sparkContext: Option[SparkContext] = None): Unit = {

    val leaves = buildPipeline(jsonString)
    
    println(s"running $jobId/$batchId: $comment")

    command match {
      case "show" => // show plan only
      case "run" =>
        val sc = if(sparkContext.isDefined) {
          sparkContext.get
        } else {
          val sparkConf = new SparkConf().setAppName(name + s"::$jobId::$batchId")
          new SparkContext(sparkConf)
        }
        setSparkContext(sc)
        val sqlContext = new HiveContext(sc)
        /** invoke the leaf processors */
        leaves.foreach(_.process(sqlContext))
    }

  }

}
