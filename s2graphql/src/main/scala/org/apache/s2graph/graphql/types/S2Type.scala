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

  case class AddVertexParam(timestamp: Long,
                            id: Any,
                            serviceName: String,
                            columnName: String,
                            props: Map[String, Any])

  case class AddEdgeParam(ts: Long,
                          from: Any,
                          to: Any,
                          direction: String,
                          props: Map[String, Any])

  // management params
  case class ServiceColumnParam(serviceName: String,
                                columnName: String,
                                props: Seq[Prop] = Nil)


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

  def makeInputPartialEdgeParamType(label: Label): InputObjectType[AddEdgeParam] = {
    lazy val InputPropsType = InputObjectType[Map[String, ScalarType[_]]](
      s"Input_${label.label}_edge_props",
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

    InputObjectType[AddEdgeParam](
      s"Input_${label.label}_edge_mutate",
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
      val columnMetasKv = column.metasWithoutCache.filter(ColumnMeta.isValid).map { columnMeta => columnMeta.name -> columnMeta.dataType }
      val reservedFields = List("id" -> column.columnType, "timestamp" -> "long")

      val vertexPropFields = makePropFields(reservedFields ++ columnMetasKv)

      lazy val ConnectedLabelType = ObjectType(
        s"Input_${service.serviceName}_${column.columnName}",
        () => fields[GraphRepository, Any](vertexPropFields ++ connectedLabelFields: _*)
      )

      Field(column.columnName,
        ListType(ConnectedLabelType),
        arguments = List(
          Argument("id", OptionInputType(s2TypeToScalarType(column.columnType))),
          Argument("ids", OptionInputType(ListInputType(s2TypeToScalarType(column.columnType)))),
          Argument("search", OptionInputType(StringType))
        ),
        description = Option("desc here"),
        resolve = c => {
          val ids = c.argOpt[Any]("id").toSeq ++ c.argOpt[List[Any]]("ids").toList.flatten
          val vertices = ids.map(vid => c.ctx.toVertex(vid, column))

          val selectedFields = c.astFields.flatMap { f =>
            f.selections.map(s => s.asInstanceOf[sangria.ast.Field].name)
          }

          if (selectedFields.forall(_ == "id")) vertices else repo.getVertices(vertices) // fill props
        }
      ): Field[GraphRepository, Any]
    }

    columnsOnService
  }

  def makeLabelField(label: Label, connectedLabelFields: => List[Field[GraphRepository, Any]]): Field[GraphRepository, Any] = {
    val labelColumns = List("from" -> label.srcColumnType, "to" -> label.tgtColumnType, "timestamp" -> "long")
    val labelProps = label.labelMetas.map { lm => lm.name -> lm.dataType }

    lazy val EdgeType = ObjectType(
      s"Label_${label.label}",
      () => fields[GraphRepository, Any](edgeFields ++ connectedLabelFields: _*)
    )

    lazy val edgeFields: List[Field[GraphRepository, Any]] = makePropFields(labelColumns ++ labelProps)
    lazy val edgeTypeField: Field[GraphRepository, Any] = Field(
      s"${label.label}",
      ListType(EdgeType),
      arguments = DirArg :: Nil,
      description = Some("edges"),
      resolve = { c =>
        val dir = c.argOpt("direction").getOrElse("out")

        val vertex: S2VertexLike = c.value match {
          case v: S2VertexLike => v
          case e: S2Edge => if (dir == "out") e.tgtVertex else e.srcVertex
          //          case vp: ServiceParam =>
          //            if (dir == "out") c.ctx.toVertex(label.tgtColumn, vp)
          //            else c.ctx.toVertex(label.srcColumn, vp)
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
    lazy val serviceFields = paddingDummyField(makeServiceField(service, repo.allLabels()))
    lazy val ServiceType = ObjectType(
      s"Service_${service.serviceName}",
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
  lazy val addVertexArg = {
    val fields = repo.allServices.map { service =>
      val inputFields = service.serviceColumns(false).map { serviceColumn =>
        val idField = InputField("id", s2TypeToScalarType(serviceColumn.columnType))
        val propFields = serviceColumn.metasWithoutCache.filter(ColumnMeta.isValid).map { lm =>
          InputField(lm.name, OptionInputType(s2TypeToScalarType(lm.dataType)))
        }

        val vertexMutateType = InputObjectType[Map[String, Any]](
          s"Input_${service.serviceName}_${serviceColumn.columnName}_vertex_mutate",
          description = "desc here",
          () => idField :: propFields
        )

        InputField[Any](serviceColumn.columnName, vertexMutateType)
      }

      val tpe = InputObjectType[Any](
        s"${service.serviceName}_param", fields = DummyInputField :: inputFields.toList
      )

      InputField(service.serviceName, OptionInputType(tpe))
    }

    InputObjectType[Vector[AddVertexParam]]("vertex_input", fields = DummyInputField :: fields)
  }

  lazy val addEdgeArg = repo.allLabels().map { label =>
    val inputPartialEdgeParamType = makeInputPartialEdgeParamType(label)
    Argument(label.label, OptionInputType(inputPartialEdgeParamType))
  }

  lazy val addEdgesArg = repo.allLabels().map { label =>
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
      ListType(MutateResponseType),
      arguments = Argument("vertex", ListInputType(addVertexArg)) :: Nil,
      resolve = c => {
        val vertices = repo.parseAddVertexParam(c.args)
        c.ctx.addVertex(vertices)
      }
    ),
    Field("addEdge",
      OptionType(MutateResponseType),
      arguments = addEdgeArg,
      resolve = c => c.ctx.addEdge(c.args)
    )
  )
}
