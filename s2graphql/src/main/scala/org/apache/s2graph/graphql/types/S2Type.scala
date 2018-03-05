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

package org.apache.s2graph.graphql.types

import org.apache.s2graph.core.Management.JsonModel.{Index, Prop}
import org.apache.s2graph.core._
import org.apache.s2graph.core.mysqls._
import org.apache.s2graph.core.storage.MutateResponse
import org.apache.s2graph.graphql._
import org.apache.s2graph.graphql.repository.GraphRepository
import org.apache.s2graph.graphql.types.S2ManagementType._
import play.api.libs.json.JsValue
import sangria.marshalling.{CoercedScalaResultMarshaller, FromInput}
import sangria.schema._

import scala.language.existentials
import scala.util.{Failure, Success, Try}
import org.apache.s2graph.graphql.marshaller._

object S2Type {
  case class PartialServiceColumn(serviceName: String,
                                  columnName: String,
                                  props: Seq[Prop] = Nil)

  case class PartialServiceParam(service: Service,
                                 vid: Any)

  case class PartialVertexParam(ts: Long,
                                id: Any,
                                props: Map[String, Any])

  case class PartialServiceVertexParam(columnName: String,
                                       vertexParam: PartialVertexParam)

  case class PartialEdgeParam(ts: Long,
                              from: Any,
                              to: Any,
                              direction: String,
                              props: Map[String, Any])

  val DirArg = Argument("direction", OptionInputType(DirectionType), "desc here", defaultValue = "out")

  def makePropFields(fieldNameWithTypes: List[(String, String)]): List[Field[GraphRepository, Any]] = {
    def makeField[A](name: String, cType: String, tpe: ScalarType[A]): Field[GraphRepository, Any] =
      Field(name,
        OptionType(tpe),
        description = Option("desc here"),
        resolve = _.value match {
          case v: S2VertexLike =>
            name match {
              case "timestamp" => v.ts.asInstanceOf[A]
              case _ =>
                val innerVal = v.propertyValue(name).get
                JSONParser.innerValToAny(innerVal, cType).asInstanceOf[A]
            }

          case e: S2EdgeLike =>
            name match {
              case "timestamp" => e.ts.asInstanceOf[A]
              case _ =>
                val innerVal = e.propertyValue(name).get.innerVal
                JSONParser.innerValToAny(innerVal, cType).asInstanceOf[A]
            }

          case _ => throw new RuntimeException("Error !!!!")
        })

    fieldNameWithTypes.map { case (cName, cType) =>
      cType match {
        case "boolean" | "bool" => makeField[Boolean](cName, cType, BooleanType)
        case "string" | "str" | "s" => makeField[String](cName, cType, StringType)
        case "int" | "integer" | "i" | "int32" | "integer32" => makeField[Int](cName, cType, IntType)
        case "long" | "l" | "int64" | "integer64" => makeField[Long](cName, cType, LongType)
        case "double" | "d" | "float64" | "float" | "f" | "float32" => makeField[Double](cName, cType, FloatType)
        case _ => throw new RuntimeException(s"Cannot support data type: ${cType}")
      }
    }
  }

  def makeInputPartialVertexParamType(service: Service,
                                      serviceColumn: ServiceColumn): InputObjectType[PartialVertexParam] = {
    lazy val InputPropsType = InputObjectType[Map[String, ScalarType[_]]](
      s"${service.serviceName}_${serviceColumn.columnName}_props",
      description = "desc here",
      () => serviceColumn.metas.filter(ColumnMeta.isValid).map { lm =>
        InputField(lm.name, OptionInputType(s2TypeToScalarType(lm.dataType)))
      }
    )

    lazy val fields = List(
      InputField("_", OptionInputType(LongType))
    )

    InputObjectType[PartialVertexParam](
      s"${service.serviceName}_on_${serviceColumn.columnName}_mutate",
      description = "desc here",
      () =>
        if (!serviceColumn.metas.exists(ColumnMeta.isValid)) fields
        else List(InputField("props", OptionInputType(InputPropsType)))
    )
  }

  def makeInputPartialEdgeParamType(label: Label): InputObjectType[PartialEdgeParam] = {
    lazy val InputPropsType = InputObjectType[Map[String, ScalarType[_]]](
      s"${label.label}_props",
      description = "desc here",
      () => label.labelMetaSet.toList.map { lm =>
        InputField(lm.name, OptionInputType(s2TypeToScalarType(lm.dataType)))
      }
    )

    lazy val labelFields = List(
      InputField("timestamp", OptionInputType(LongType)),
      InputField("from", s2TypeToScalarType(label.srcColumnType)),
      InputField("to", s2TypeToScalarType(label.srcColumnType)),
      InputField("direction", OptionInputType(DirectionType))
    )

    InputObjectType[PartialEdgeParam](
      s"${label.label}_mutate",
      description = "desc here",
      () =>
        if (label.labelMetaSet.isEmpty) labelFields
        else labelFields ++ Seq(InputField("props", OptionInputType(InputPropsType)))
    )
  }

  def makeServiceField(service: Service, allLabels: List[Label])(implicit repo: GraphRepository): List[Field[GraphRepository, Any]] = {
    lazy val columnsOnService = service.serviceColumns(false).toList.map { column =>
      val connectedLabels = allLabels.filter { lb =>
        column.id.get == lb.srcColumn.id.get || column.id.get == lb.tgtColumn.id.get
      }.distinct

      lazy val connectedLabelFields: List[Field[GraphRepository, Any]] = connectedLabels.map(makeLabelField(_, connectedLabelFields))
      val columnMetasKv = column.metas.filter(ColumnMeta.isValid).map { columnMeta => columnMeta.name -> columnMeta.dataType }
      val reservedFields = List("id" -> column.columnType, "timestamp" -> "long")

      val vertexPropFields = makePropFields(reservedFields ++ columnMetasKv)
      lazy val LabelType = ObjectType(
        s"${service.serviceName}_${column.columnName}",
        () => fields[GraphRepository, Any](vertexPropFields ++ connectedLabelFields: _*)
      )

      Field(column.columnName,
        ListType(LabelType),
        arguments = List(
          Argument("id", OptionInputType(s2TypeToScalarType(column.columnType))),
          Argument("ids", OptionInputType(ListInputType(s2TypeToScalarType(column.columnType)))),
          Argument("search", OptionInputType(StringType))
        ),
        description = Option("desc here"),
        resolve = c => {
          println(c.parentType.name)
          c.astFields.foreach { f =>
            f.selections.foreach(s => {
              println(s)
            })
          }
          val id = c.argOpt[Any]("id").toSeq
          val ids = c.argOpt[List[Any]]("ids").toList.flatten
          val svc = c.ctx.findServiceByName(service.serviceName).get

          val vids = (id ++ ids).map { vid =>
            val vp = PartialServiceParam(svc, vid)
            c.ctx.partialServiceParamToVertex(column, vp)
          }

          repo.getVertex(vids.head)
        }
      ): Field[GraphRepository, Any]
    }

    columnsOnService
  }

  def makeLabelField(label: Label, connectedLabelFields: => List[Field[GraphRepository, Any]]): Field[GraphRepository, Any] = {
    val labelColumns = List("from" -> label.srcColumnType, "to" -> label.tgtColumnType, "timestamp" -> "long")
    val labelProps = label.labelMetas.map { lm => lm.name -> lm.dataType }

    lazy val EdgeType = ObjectType(label.label, () => fields[GraphRepository, Any](edgeFields ++ connectedLabelFields: _*))
    lazy val edgeFields: List[Field[GraphRepository, Any]] = makePropFields(labelColumns ++ labelProps)
    lazy val edgeTypeField: Field[GraphRepository, Any] = Field(
      label.label,
      ListType(EdgeType),
      arguments = DirArg :: Nil,
      description = Some("edges"),
      resolve = { c =>
        val dir = c.argOpt("direction").getOrElse("out")

        val vertex: S2VertexLike = c.value match {
          case v: S2VertexLike => v
          case e: S2Edge => if (dir == "out") e.tgtVertex else e.srcVertex
          case vp: PartialServiceParam =>
            if (dir == "out") c.ctx.partialServiceParamToVertex(label.tgtColumn, vp)
            else c.ctx.partialServiceParamToVertex(label.srcColumn, vp)
        }

        c.ctx.getEdges(vertex, label, dir)
      }
    )

    edgeTypeField
  }

}

class S2Type(repo: GraphRepository) {

  import S2Type._

  implicit val graphRepository = repo

  /**
    * fields
    */
  lazy val serviceFields: List[Field[GraphRepository, Any]] = repo.allServices.map { service =>
    lazy val serviceFields = paddingDummyField(makeServiceField(service, repo.allLabels))
    lazy val ServiceType = ObjectType(
      service.serviceName,
      fields[GraphRepository, Any](serviceFields: _*)
    )

    Field(
      service.serviceName,
      ServiceType,
      description = Some(s"serviceName: ${service.serviceName}"),
      resolve = _ => service
    ): Field[GraphRepository, Any]
  }

  /**
    * arguments
    */
  lazy val addVertexArg = repo.allServices.map { service =>
    val columnFields = service.serviceColumns(false).map { serviceColumn =>

      val columnMetas = serviceColumn.metas.filter(ColumnMeta.isValid).map { lm =>
        InputField(lm.name, OptionInputType(s2TypeToScalarType(lm.dataType)))
      }

      lazy val InputPropsType = InputObjectType[Map[String, ScalarType[_]]](
        s"${service.serviceName}_${serviceColumn.columnName}_props",
        description = "desc here",
        () => if (columnMetas.isEmpty) List(DummyInputField) else columnMetas.toList
      )

      val tpe = InputObjectType[PartialServiceVertexParam](
        serviceColumn.columnName,
        fields = List(
          InputField("id", s2TypeToScalarType(serviceColumn.columnType)),
          InputField("timestamp", OptionInputType(LongType)),
          InputField("props", OptionInputType(InputPropsType))
        )
      )

      InputField(serviceColumn.columnName, OptionInputType(tpe))
    }

    val vertexParamType = InputObjectType[Vector[PartialServiceVertexParam]](
      s"${service.serviceName}_column",
      description = "desc here",
      fields = if (columnFields.isEmpty) List(DummyInputField) else columnFields.toList
    )

    Argument(service.serviceName, OptionInputType(vertexParamType))
  }

  lazy val addVerticesArg = repo.allServices.flatMap { service =>
    service.serviceColumns(false).map { serviceColumn =>
      val inputPartialVertexParamType = makeInputPartialVertexParamType(service, serviceColumn)
      Argument(serviceColumn.columnName, OptionInputType(ListInputType(inputPartialVertexParamType)))
    }
  }

  lazy val addEdgeArg = repo.allLabels.map { label =>
    val inputPartialEdgeParamType = makeInputPartialEdgeParamType(label)
    Argument(label.label, OptionInputType(inputPartialEdgeParamType))
  }

  lazy val addEdgesArg = repo.allLabels.map { label =>
    val inputPartialEdgeParamType = makeInputPartialEdgeParamType(label)
    Argument(label.label, OptionInputType(ListInputType(inputPartialEdgeParamType)))
  }

  /**
    * Query fields
    * Provide s2graph query / mutate API
    * - Fields is created(or changed) for metadata is changed.
    */
  lazy val queryFields = serviceFields

  lazy val mutationFields: List[Field[GraphRepository, Any]] = List(
    Field("addVertex",
      OptionType(MutateResponseType),
      arguments = addVertexArg,
      resolve = c => c.ctx.addVertex(c.args)
    ),
    Field("addVertices",
      ListType(MutateResponseType),
      arguments = addVerticesArg,
      resolve = c => c.ctx.addVertices(c.args)
    ),
    Field("addEdge",
      OptionType(MutateResponseType),
      arguments = addEdgeArg,
      resolve = c => c.ctx.addEdge(c.args)
    ),
    Field("addEdges",
      ListType(MutateResponseType),
      arguments = addEdgesArg,
      resolve = c => c.ctx.addEdges(c.args)
    )
  )
}
