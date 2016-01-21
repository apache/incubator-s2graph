package org.apache.s2graph.lambda

import java.util.UUID

import org.apache.s2graph.lambda.source.StreamContainer
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag

case class JobDesc(name: String, processors: List[ProcessorDesc], streamContainer: Option[ProcessorDesc], root: Option[String], comment: Option[String])

case class ProcessorDesc(`class`: String, params: Option[JsonUtil.JVal], id: Option[String], pid: Option[String], pids: Option[Seq[String]])

case class ProcessorFactory(className: String, paramsAsJValue: Option[JsonUtil.JVal], context: Context) {

  private val constructor = Class.forName(className).getConstructors.head

  def getInstance: BaseDataProcessor[Data, Data] = {
    val instance = constructor.getParameterTypes match {
      case Array() =>
        constructor.newInstance()
      case Array(pClass) if classOf[Params].isAssignableFrom(pClass) =>
        val params = JsonUtil.extract(paramsAsJValue.getOrElse(JsonUtil.emptyJsonValue))(ClassTag(pClass)).asInstanceOf[Params]
        try {
          constructor.newInstance(params)
        } catch {
          case ex: java.lang.reflect.InvocationTargetException =>
            println(s"cause: ${ex.getCause}")
            throw ex
        }
      case _ =>
        require(false,
          Seq(constructor.getParameterTypes.toSeq, className, paramsAsJValue).map(_.toString).mkString(","))
    }
    instance.asInstanceOf[BaseDataProcessor[Data, Data]].setContext(context)
  }

}

object Launcher extends Visualization {

  def main(args: Array[String]): Unit = {

    require(args.length >= 2)

    val Array(command, jsonString) = args.slice(0, 2)

    launch(jsonString, command)
  }

  def buildPipeline(processors: List[ProcessorDesc], context: Context, source: BaseDataProcessor[_ <: Data, _ <: Data] = null)
  : List[BaseDataProcessor[Data, Data]] = {

    /** instantiation */
    val instances = processors.zipWithIndex.map { case (p, order) =>
      val id = p.id.getOrElse(UUID.randomUUID().toString)
      val pids = (p.pid, p.pids) match {
        case (None, None) => Seq.empty[String]
        case (Some(a), None) => Seq(a)
        case (None, Some(b)) => b
        case (Some(a), Some(b)) => {
          println("warn: Something wrong")
          Seq(a) ++ b
        }
      }
      val instance = ProcessorFactory(p.`class`, p.params, context).getInstance
      (order, id, pids, instance)
    }

    /** add dependencies for each processor */
    val keyByOrder = instances.map(x => x._1 -> x).toMap
    val keyById = instances.map(x => x._2 -> x).toMap

    val rootId = "root"

    instances.foreach { case (order, id, pids, instance) =>
      val predecessors = pids match {
        case s if s.isEmpty && order == 0 && source == null => Seq.empty[BaseDataProcessor[_, _]]
        case s if s.isEmpty && order == 0 && source != null => Seq(source)
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

  def launch(jsonString: String, command: String, givenSparkContext: Option[SparkContext] = None): Unit = {
    /** parsing the job description */
    val jobDesc = JsonUtil.extract[JobDesc](jsonString)

    /** show the job desciprion */
    println(JsonUtil.toPrettyJsonString(jobDesc))

    val sparkContext = givenSparkContext.getOrElse {
      val sparkConf = new SparkConf()
      new SparkContext(sparkConf)
    }

    val context = Context(jobDesc.name, jobDesc.root.orNull, jobDesc.comment.orNull, sparkContext)

    val streamContainer = jobDesc.streamContainer.map { desc =>
      ProcessorFactory(desc.`class`, desc.params, context).getInstance.asInstanceOf[StreamContainer[_]]
    }

    println(s"running ${context.jobId}/${context.batchId}: ${context.comment}")

    streamContainer match {
      case Some(container) =>
        /** streaming job */
        val leaves = buildPipeline(jobDesc.processors, context, container.frontEnd)
        if (command == "run") {
          val ssc = container.streamingContext
          container.foreachBatch {
            leaves.foreach(_.invalidateCache())
            leaves.foreach(_.process())
          }

          ssc.start()
          container.getParams.timeout match {
            case Some(timeout) =>
              Thread.sleep(timeout)
              ssc.stop(true, true)
            case None =>
              ssc.awaitTermination()
          }
        }
      case _ =>
        /** batch job */
        val leaves = buildPipeline(jobDesc.processors, context)
        if (command == "run") {
          leaves.foreach(_.process())
        }
    }
  }
}
