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
import org.apache.s2graph.graphql.repository.GraphRepository
import sangria.schema._

import scala.language.existentials
import org.apache.s2graph.graphql.marshaller._

object S2Type {

  case class AddVertexParam(timestamp: Long,
                            id: Any,
                            columnName: String,
                            props: Map[String, Any])

  case class AddEdgeParam(ts: Long,
                          from: Any,
                          to: Any,
                          direction: String,
                          props: Map[String, Any])

  // Management params
  case class ServiceColumnParam(serviceName: String,
                                columnName: String,
                                props: Seq[Prop] = Nil)

  def makeField[A](name: String, cType: String, tpe: ScalarType[A]): Field[GraphRepository, Any] =
    Field(name,
      OptionType(tpe),
      description = Option("desc here"),
      resolve = c => c.value match {
        case v: S2VertexLike => name match {
          case "timestamp" => v.ts.asInstanceOf[A]
          case _ =>
            val innerVal = v.propertyValue(name).get
            JSONParser.innerValToAny(innerVal, cType).asInstanceOf[A]
        }
        case e: S2EdgeLike => name match {
          case "timestamp" => e.ts.asInstanceOf[A]
          case "direction" => e.getDirection().asInstanceOf[A]
          case _ =>
            val innerVal = e.propertyValue(name).get.innerVal
            JSONParser.innerValToAny(innerVal, cType).asInstanceOf[A]
        }
        case _ =>
          throw new RuntimeException(s"Error on resolving field: ${name}, ${cType}, ${c.value.getClass}")
      }
    )

  def makePropField(cName: String, cType: String): Field[GraphRepository, Any] = cType match {
    case "boolean" | "bool" => makeField[Boolean](cName, cType, BooleanType)
    case "string" | "str" | "s" => makeField[String](cName, cType, StringType)
    case "int" | "integer" | "i" | "int32" | "integer32" => makeField[Int](cName, cType, IntType)
    case "long" | "l" | "int64" | "integer64" => makeField[Long](cName, cType, LongType)
    case "double" | "d" => makeField[Double](cName, cType, FloatType)
    case "float64" | "float" | "f" | "float32" => makeField[Double](cName, "double", FloatType)
    case _ => throw new RuntimeException(s"Cannot support data type: ${cType}")
  }

  def makeInputFieldsOnService(service: Service): Seq[InputField[Any]] = {
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

      InputField[Any](serviceColumn.columnName, OptionInputType(ListInputType(vertexMutateType)))
    }

    inputFields
  }

  def makeInputFieldsOnLabel(label: Label): Seq[InputField[Any]] = {
    val propFields = label.labelMetaSet.toList.map { lm =>
      InputField(lm.name, OptionInputType(s2TypeToScalarType(lm.dataType)))
    }

    val labelFields = List(
      InputField("timestamp", OptionInputType(LongType)),
      InputField("from", s2TypeToScalarType(label.srcColumnType)),
      InputField("to", s2TypeToScalarType(label.srcColumnType)),
      InputField("direction", OptionInputType(DirectionType))
    )

    labelFields.asInstanceOf[Seq[InputField[Any]]] ++ propFields.asInstanceOf[Seq[InputField[Any]]]
  }

  def makeServiceColumnFields(column: ServiceColumn): List[Field[GraphRepository, Any]] = {
    val reservedFields = List("id" -> column.columnType, "timestamp" -> "long")
    val columnMetasKv = column.metasWithoutCache.filter(ColumnMeta.isValid).map { columnMeta => columnMeta.name -> columnMeta.dataType }

    (reservedFields ++ columnMetasKv).map { case (k, v) => makePropField(k, v) }
  }

  def makeServiceField(service: Service, allLabels: List[Label])(implicit repo: GraphRepository): List[Field[GraphRepository, Any]] = {
    lazy val columnsOnService = service.serviceColumns(false).toList.map { column =>

      val outLabels = allLabels.filter { lb => column.id.get == lb.srcColumn.id.get }.distinct
      val inLabels = allLabels.filter { lb => column.id.get == lb.tgtColumn.id.get }.distinct

      lazy val vertexPropFields = makeServiceColumnFields(column)

      lazy val outLabelFields: List[Field[GraphRepository, Any]] =
        outLabels.map(l => makeLabelField("out", l, allLabels))

      lazy val inLabelFields: List[Field[GraphRepository, Any]] =
        inLabels.map(l => makeLabelField("in", l, allLabels))

      lazy val connectedLabelType = ObjectType(
        s"Input_${service.serviceName}_${column.columnName}",
        () => fields[GraphRepository, Any](vertexPropFields ++ (inLabelFields ++ outLabelFields): _*)
      )

      val v = Field(column.columnName,
        ListType(connectedLabelType),
        arguments = List(
          Argument("id", OptionInputType(s2TypeToScalarType(column.columnType))),
          Argument("ids", OptionInputType(ListInputType(s2TypeToScalarType(column.columnType)))),
          Argument("search", OptionInputType(StringType))
        ),
        description = Option("desc here"),
        resolve = c => {
          implicit val ec = c.ctx.ec

          val ids = c.argOpt[Any]("id").toSeq ++ c.argOpt[List[Any]]("ids").toList.flatten
          val vertices = ids.map(vid => c.ctx.toS2VertexLike(vid, column))

          val columnFields = column.metasInvMap.keySet
          val selectedFields = c.astFields.flatMap { f =>
            f.selections.map(s => s.asInstanceOf[sangria.ast.Field].name)
          }

          val passFetchVertex = selectedFields.forall(f => f == "id" || !columnFields(f))

          if (passFetchVertex) scala.concurrent.Future.successful(vertices)
          else repo.getVertices(vertices) // fill props
        }
      ): Field[GraphRepository, Any]

      v
    }

    columnsOnService
  }

  def fillPartialVertex(vertex: S2VertexLike,
                        column: ServiceColumn,
                        c: Context[GraphRepository, Any]): scala.concurrent.Future[S2VertexLike] = {
    implicit val ec = c.ctx.ec

    val columnFields = column.metasInvMap.keySet
    val selectedFields = c.astFields.flatMap { f =>
      f.selections.map(s => s.asInstanceOf[sangria.ast.Field].name)
    }

    // Vertex on edge has invalid `serviceColumn` info
    lazy val newVertex = c.ctx.toS2VertexLike(vertex.innerId, column)

    val passFetchVertex = selectedFields.forall(f => f == "id" || !columnFields(f))

    if (passFetchVertex) scala.concurrent.Future.successful(vertex)
    else c.ctx.getVertices(Seq(newVertex)).map(_.head) // fill props
  }

  def makeLabelField(dir: String, label: Label, allLabels: List[Label]): Field[GraphRepository, Any] = {
    val labelReserved = List("direction" -> "string", "timestamp" -> "long")
    val labelProps = label.labelMetas.map { lm => lm.name -> lm.dataType }

    lazy val edgeFields: List[Field[GraphRepository, Any]] =
      (labelReserved ++ labelProps).map { case (k, v) => makePropField(k, v) }

    val column = if (dir == "out") label.tgtColumn else label.srcColumn

    lazy val toType = ObjectType(s"Label_${label.label}_${column.columnName}", () => {
      lazy val linked = if (dir == "out") {
        val inLabels = allLabels.filter { lb => column.id.get == lb.tgtColumn.id.get }.distinct
        inLabels.map(l => makeLabelField("in", l, allLabels))
      } else {
        val outLabels = allLabels.filter { lb => column.id.get == lb.srcColumn.id.get }.distinct
        outLabels.map(l => makeLabelField("out", l, allLabels))
      }

      makeServiceColumnFields(column) ++ linked
    })

    lazy val toField: Field[GraphRepository, Any] = Field(column.columnName, toType, resolve = c => {
      val vertex = if (dir == "out") {
        c.value.asInstanceOf[S2EdgeLike].tgtVertex
      } else {
        c.value.asInstanceOf[S2EdgeLike].srcVertex
      }

      fillPartialVertex(vertex, column, c)
    })

    lazy val EdgeType = ObjectType(
      s"Label_${label.label}_${column.columnName}_${dir}",
      () => fields[GraphRepository, Any](List(toField) ++ edgeFields: _*)
    )

    lazy val edgeTypeField: Field[GraphRepository, Any] = Field(
      s"${label.label}",
      ListType(EdgeType),
      arguments = Argument("direction", OptionInputType(DirectionType), "desc here", defaultValue = "out") :: Nil,
      description = Some("fetch edges"),
      resolve = { c =>
        val vertex: S2VertexLike = c.value match {
          case v: S2VertexLike => v
          case _ => throw new IllegalArgumentException(s"ERROR: ${c.value.getClass}")
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
    lazy val serviceFields = DummyObjectTypeField :: makeServiceField(service, repo.allLabels())

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
    val serviceArguments = repo.allServices().map { service =>
      val serviceFields = DummyInputField +: makeInputFieldsOnService(service)

      val ServiceInputType = InputObjectType[List[AddVertexParam]](
        s"Input_vertex_${service.serviceName}_param",
        () => serviceFields.toList
      )
      Argument(service.serviceName, OptionInputType(ServiceInputType))
    }

    serviceArguments
  }

  lazy val addEdgeArg = {
    val labelArguments = repo.allLabels().map { label =>
      val labelFields = DummyInputField +: makeInputFieldsOnLabel(label)
      val labelInputType = InputObjectType[AddEdgeParam](
        s"Input_label_${label.label}_param",
        () => labelFields.toList
      )

      Argument(label.label, OptionInputType(ListInputType(labelInputType)))
    }

    labelArguments
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
      arguments = addVertexArg,
      resolve = c => {
        val vertices = c.args.raw.keys.flatMap { serviceName =>
          val addVertexParams = c.arg[List[AddVertexParam]](serviceName)
          addVertexParams.map { param =>
            repo.toS2VertexLike(serviceName, param)
          }
        }

        c.ctx.addVertices(vertices.toSeq)
      }
    ),
    Field("addEdge",
      ListType(MutateResponseType),
      arguments = addEdgeArg,
      resolve = c => {
        val edges = c.args.raw.keys.flatMap { labelName =>
          val addEdgeParams = c.arg[Vector[AddEdgeParam]](labelName)
          addEdgeParams.map { param =>
            repo.toS2EdgeLike(labelName, param)
          }
        }

        c.ctx.addEdges(edges.toSeq)
      }
    )
  )
}
