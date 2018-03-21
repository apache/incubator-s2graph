package org.apache.s2graph.graphql

import org.apache.s2graph.core.Management.JsonModel._
import org.apache.s2graph.graphql.types.S2Type._
import sangria.marshalling._

package object marshaller {
  def unwrapOption(map: Map[String, Any]): Map[String, Any] = map.map {
    case (k, v: Some[_]) => v.get match {
      case m: Map[_, _] => k -> unwrapOption(m.asInstanceOf[Map[String, Any]])
      case _ => k -> v.get
    }
    case (k, v: Map[_, _]) => k -> unwrapOption(v.asInstanceOf[Map[String, Any]])
    case org@_ => org
  }

  implicit object AddVertexParamFromInput extends FromInput[Vector[AddVertexParam]] {
    val marshaller = CoercedScalaResultMarshaller.default

    def fromResult(node: marshaller.Node) = {
      val currentTime = System.currentTimeMillis()
      val inenrMap = unwrapOption(node.asInstanceOf[Map[String, Any]])

      val params = inenrMap.flatMap { case (serviceName, serviceColumnMap) =>
        serviceColumnMap.asInstanceOf[Map[String, Any]].map  { case (columnName, vertexParamMap) =>
          val propMap = vertexParamMap.asInstanceOf[Map[String, Any]]
          val id = propMap("id")
          val ts = propMap.getOrElse("timestamp", currentTime).asInstanceOf[Long]

          AddVertexParam(ts, id, serviceName, columnName, propMap.filterKeys(k => k != "timestamp" || k != "id"))
        }
      }

      params.toVector
    }
  }

  implicit object IndexFromInput extends FromInput[Index] {
    val marshaller = CoercedScalaResultMarshaller.default

    def fromResult(node: marshaller.Node) = {
      val input = node.asInstanceOf[Map[String, Any]]
      Index(input("name").asInstanceOf[String], input("propNames").asInstanceOf[Seq[String]])
    }
  }

  implicit object PropFromInput extends FromInput[Prop] {
    val marshaller = CoercedScalaResultMarshaller.default

    def fromResult(node: marshaller.Node) = {
      val input = node.asInstanceOf[Map[String, Any]]

      val name = input("name").asInstanceOf[String]
      val defaultValue = input("defaultValue").asInstanceOf[String]
      val dataType = input("dataType").asInstanceOf[String]
      val storeInGlobalIndex = input("storeInGlobalIndex").asInstanceOf[Boolean]

      Prop(name, defaultValue, dataType, storeInGlobalIndex)
    }
  }

  implicit object ServiceColumnParamFromInput extends FromInput[ServiceColumnParam] {
    val marshaller = CoercedScalaResultMarshaller.default

    def fromResult(node: marshaller.Node) = ServiceColumnParamsFromInput.fromResult(node).head
  }

  implicit object ServiceColumnParamsFromInput extends FromInput[Vector[ServiceColumnParam]] {
    val marshaller = CoercedScalaResultMarshaller.default

    def fromResult(node: marshaller.Node) = {
      val input = unwrapOption(node.asInstanceOf[Map[String, Any]])

      val partialServiceColumns = input.map { case (serviceName, serviceColumnMap) =>
        val innerMap = serviceColumnMap.asInstanceOf[Map[String, Any]]
        val columnName = innerMap("columnName").asInstanceOf[String]
        val props = innerMap.get("props").toSeq.flatMap { case vs: Vector[_] =>
          vs.map(PropFromInput.fromResult)
        }

        ServiceColumnParam(serviceName, columnName, props)
      }

      partialServiceColumns.toVector
    }
  }

  implicit object AddEdgeParamFromInput extends FromInput[AddEdgeParam] {
    val marshaller = CoercedScalaResultMarshaller.default

    def fromResult(node: marshaller.Node) = {
      val inputMap = node.asInstanceOf[Map[String, Any]]

      val from = inputMap("from")
      val to = inputMap("to")

      val ts = inputMap.get("timestamp") match {
        case Some(Some(v)) => v.asInstanceOf[Long]
        case _ => System.currentTimeMillis()
      }

      val dir = inputMap.get("direction") match {
        case Some(Some(v)) => v.asInstanceOf[String]
        case _ => "out"
      }

      val props = inputMap.get("props") match {
        case Some(Some(v)) => v.asInstanceOf[Map[String, Option[Any]]].filter(_._2.isDefined).mapValues(_.get)
        case _ => Map.empty[String, Any]
      }

      AddEdgeParam(ts, from, to, dir, props)
    }
  }

}
