package org.apache.s2graph.lambda

import scala.collection.mutable
import scala.reflect._

/**
  * BaseDataProcessor, the basic abstraction in S2Lambda, represents a data processor
  * which generates output data `O` using input data `I`.
  */
abstract class BaseDataProcessor[I <: Data : ClassTag, O <: Data : ClassTag](params: Params)
    extends Serializable with Logging {

  def this() = this(Params.emptyOrNotGivenParams)

  final private var cached: O = _
  final private var predecessors: Set[BaseDataProcessor[_, _]] = _
  final private var depth: Int = -1
  final private var order: Int = -1
  final protected var context: Context = _
  final protected var predecessorData: PredecessorData = Data.emptyPredecessorData
  final protected val iClass: Class[_] = classTag[I].runtimeClass
  final protected val oClass: Class[_] = classTag[O].runtimeClass

  final protected var inputKeyArray: Array[(String, Class[_])] = {
    if(iClass == classOf[PredecessorData]) null // not determined yet, see setPredecessors
    else iClass.getDeclaredFields.map { f => f.getName -> f.getType }
  }

  final protected var outputKeyArray: Array[(String, Class[_])] = {
    if(oClass == classOf[PredecessorData]) null // not determined yet, see setPredecessors.
    else oClass.getDeclaredFields.map { f => f.getName -> f.getType }
  }

  final def setPredecessors(predecessors: (BaseDataProcessor[_, _])*): this.type = {

    require(this.predecessors == null, "Do not call this function twice")

    this.predecessors = predecessors.toSet
    val predecessorsOutputKeys = predecessors.flatMap(_.outputKeyArray)

    val duplications = predecessorsOutputKeys.groupBy(_._1).map { x => x._2.length -> x }.filter(_._1 > 1)
    require(duplications.isEmpty, s"duplications: $duplications")

    if(iClass == classOf[PredecessorData]) inputKeyArray = predecessorsOutputKeys.toArray
    if(oClass == classOf[PredecessorData]) outputKeyArray = predecessorsOutputKeys.toArray

    val occ = new mutable.HashMap[String, Class[_]]

    inputKeyArray.foreach { case(stringKey, requiredTypeClass) =>
      if(!classOf[Option[_]].isAssignableFrom(requiredTypeClass))
        occ(stringKey) = requiredTypeClass
    }

    predecessorsOutputKeys.foreach {
      case (stringKey, acquirableTypeClass) if occ.contains(stringKey) =>
        val requiredTypeClass = occ(stringKey)
        (requiredTypeClass, acquirableTypeClass) match {
          case (re, ac) if re.isAssignableFrom(ac) =>
            occ(stringKey) = null
          case (re, ac) if classOf[Option[_]].isAssignableFrom(re) && !classOf[Option[_]].isAssignableFrom(ac) =>
            occ(stringKey) = null
          case (re, ac) if !classOf[Option[_]].isAssignableFrom(re) && classOf[Option[_]].isAssignableFrom(ac) =>
            occ(stringKey) = null
        }
      case _ => // not required
    }

    val unassigned = occ.filter(_._2 != null)

    require(unassigned.isEmpty, {
      s"""
         |processor:
         |    $id
         |predecessors:
         |    ${predecessors.map(_.id).mkString("\n    ")}
         |unassigned keys:
         |    ${unassigned.zipWithIndex.map(x => s"${x._2}: ${x._1}").mkString("\n    ")}
         |required keys:
         |    ${inputKeyArray.zipWithIndex.map(x => s"${x._2}: ${x._1}").mkString("\n    ")}
         |acquirable keys:
         |    ${predecessorsOutputKeys.zipWithIndex.map(x => s"${x._2}: ${x._1}").mkString("\n    ")}
         |acquirable keys(per predecessor):
         |${predecessors.map { p =>
        s"""    ${p.id}
            |        ${p.outputKeyArray.zipWithIndex.map(x => s"${x._2}: ${x._1}").mkString("\n        ")}
            """}.mkString("\n")}
         |""".stripMargin
    })
    this
  }

  final def getPredecessors = if(predecessors == null) Set.empty[BaseDataProcessor[_, _]] else predecessors

  final def setDepth(depth: Int): this.type = {
    this.depth = depth
    this
  }

  final def getDepth: Int = this.depth

  final def setOrder(order: Int): this.type = {
    this.order = order
    this
  }

  final def setContext(globalContext: Context): this.type = {
    this.context = globalContext
    this
  }

  final lazy val id: String = s"${getClass.getName}@${Integer.toHexString(hashCode())}"

  override def toString: String = toString(true)

  def toString(withPad: Boolean = false): String = {
    val s = s"""processor id: $id
                |predecessors:
                |  ${if(predecessors == null) "root" else predecessors.map(_.id).mkString("\n  ")}
                |depth: $depth, order: $order, cached: ${cached != null}
                |params: ${JsonUtil.toPrettyJsonString(params)}
                |inputKeys:
                |  ${inputKeyArray.map(x => s"${x._1}:${x._2}").mkString("\n  ")}
                |outputKeys:
                |  ${outputKeyArray.map(x => s"${x._1}:${x._2}").mkString("\n  ")}
                |""".stripMargin
    if(withPad) s.split("(\r)?\n").mkString(pad, s"\n$pad", "")
    else s
  }

  final def process(): O = {
    logger.info("processing ... ")
    if(cached == null) {
      val asMap: Map[String, AnyRef] = predecessors match {
        case null => Map.empty[String, AnyRef]
        case _ =>
          predecessors.toSeq.sortBy(_.order).flatMap { predecessor =>
            predecessor.process() match {
              case out if out.getClass == classOf[PredecessorData] =>
                out.asInstanceOf[PredecessorData].asMap.asInstanceOf[Map[String, AnyRef]]
              case out =>
                out.getClass.getDeclaredFields.map { field =>
                  field.setAccessible(true)
                  field.getName -> field.get(out)
                }.toMap
            }
          }.toMap
      }

      predecessorData = PredecessorData(asMap)

      val input = {
        if(iClass == classOf[PredecessorData]) {
          predecessorData.asInstanceOf[I]
        } else {
          val keySeq = inputKeyArray.map { case (s, clazz) => (s, clazz, asMap(s)) }.toSeq
          val constructor = iClass.getConstructors.head
          val initArgs = keySeq.map { case (key, requiredTypeClass, acquiredValue) =>
            (requiredTypeClass, acquiredValue.getClass) match {
              case (re, ac) if classOf[Option[_]].isAssignableFrom(re) && !classOf[Option[_]].isAssignableFrom(ac) =>
                Some(acquiredValue)
              case (re, ac) if !classOf[Option[_]].isAssignableFrom(re) && classOf[Option[_]].isAssignableFrom(ac) =>
                acquiredValue.asInstanceOf[Option[AnyRef]].getOrElse {
                  logger.info(s"warn: no such $key, null returned instead")
                  null
                }
              case _ =>
                acquiredValue
            }
          }
          /** TODO: the case that class has multiple constructors */
          constructor.newInstance(initArgs: _*).asInstanceOf[I]
        }
      }
      val tic = System.currentTimeMillis()
      cached = processBlock(input)
      val toc = System.currentTimeMillis()
      show(f"$id - elapsed time ${(toc - tic)/1000.0}%.1f s")
    }
    logger.info("done ...")
    cached
  }

  lazy val pad = "#" * (depth + 1) + "|"

  def show(msg: => String) {
    System.out.println(pad + id + " : " + msg.toString)
  }

  def invalidateCache(): Unit = {
    cached = null.asInstanceOf[O]
    if (predecessors != null)
      predecessors.foreach(_.invalidateCache())
  }

  protected def processBlock(input: I): O

}

object BaseDataProcessor {

  def wrap[O <: Data : ClassTag](output: O): BaseDataProcessor[EmptyData, O] = {
    new BaseDataProcessor[EmptyData, O] {
      override protected def processBlock(input: EmptyData): O = output
    }
  }

  def wrap(keyValuePairs: (String, Any)*): BaseDataProcessor[EmptyData, PredecessorData] = {
    new BaseDataProcessor[EmptyData, PredecessorData] {
      override protected def processBlock(input: EmptyData): PredecessorData =
        PredecessorData(keyValuePairs.toMap)
    }
  }

}