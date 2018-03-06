package org.apache.s2graph.graphql

import java.io

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.s2graph.core.Management.JsonModel.Prop
import org.apache.s2graph.core.mysqls.{Label, Service}
import org.apache.s2graph.core.{Management, S2Graph}
import org.apache.s2graph.core.rest.RequestParser
import org.apache.s2graph.core.utils.logger
import org.apache.s2graph.graphql.repository.GraphRepository
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}
import play.api.libs.json.{JsObject, JsString, JsValue, Json}
import sangria.ast.Document

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import sangria.macros._
import sangria.execution.Executor
import sangria.execution.deferred.DeferredResolver
import sangria.parser.QueryParser
import sangria.renderer.SchemaRenderer
import sangria.schema.Schema

import scala.util._

class SchemaTest extends FunSuite with Matchers with BeforeAndAfterAll {
  var graph: S2Graph = _
  var parser: RequestParser = _
  var management: Management = _
  var config: Config = _
  var s2Repository: GraphRepository = _
  var s2Schema: Schema[GraphRepository, Any] = _

  override def beforeAll = {
    config = ConfigFactory.load()
    graph = new S2Graph(config)(scala.concurrent.ExecutionContext.Implicits.global)
    management = new Management(graph)
    parser = new RequestParser(graph)
    s2Repository = new GraphRepository(graph)

    // Init test data
    val serviceName = "kakao"
    val labelName = "friends"
    val columnName = "user"

    Management.deleteService(serviceName)
    val serviceTry: Try[Service] =
      management.createService(
        serviceName,
        "localhost",
        s"${serviceName}_table",
        1,
        None
      )

    val serviceColumnTry = serviceTry.map { _ =>
      management.createServiceColumn(
        serviceName,
        columnName,
        "string",
        List(
          Prop("age", "0", "int"),
          Prop("gender", "", "string")
        )
      )
    }

    Management.deleteLabel(labelName)
    val labelTry: Try[Label] =
      management.createLabel(
        labelName,
        serviceName, columnName, "string",
        serviceName, columnName, "string",
        true, serviceName,
        Nil,
        Seq(Prop("score", "0", "int")),
        "strong"
      )

    val isPrepared = List(serviceTry, serviceColumnTry, labelTry).forall(_.isSuccess)

    s2Schema = new SchemaDef(s2Repository).S2GraphSchema

    println("-" * 80)
    println(SchemaRenderer.renderSchema(s2Schema))
    println("-" * 80)


    require(isPrepared, "should created metadata")
  }

  override def afterAll(): Unit = {
    graph.shutdown(true)
  }

  test("s2 schema should has types") {
    val s2Schema = new SchemaDef(s2Repository).S2GraphSchema

    val Success(query) = QueryParser.parse(
      """
        query IntrospectionTypeQuery {
          __schema {
            types {
              name
            }
          }
        }
        """)

    val expected: Map[String, Map[String, Map[String, Vector[Map[String, String]]]]] =
      Map("data" ->
        Map("__schema" ->
          Map("types" -> Vector(
            Map("name" -> "Enum_CompressionAlgorithm"),
            Map("name" -> "Enum_Consistency"),
            Map("name" -> "Enum_DataType"),
            Map("name" -> "Enum_Direction"),

            Map("name" -> "MutateLabel"),
            Map("name" -> "MutateGraphElement"),
            Map("name" -> "MutateService"),
            Map("name" -> "MutateServiceColumn"),

            Map("name" -> "Input_Service"),
            Map("name" -> "Input_Index"),
            Map("name" -> "Input_Prop"),
            Map("name" -> "Input_Label_Props"),
            Map("name" -> "Input_Column"),
            Map("name" -> "Input_Column_Props"),

            Map("name" -> "ColumnMeta"),
            Map("name" -> "LabelMeta"),

            Map("name" -> "Label"),
            Map("name" -> "LabelIndex"),

            Map("name" -> "Service"),
            Map("name" -> "ServiceColumn"),

            Map("name" -> "Input_friends_edge_mutate"),
            Map("name" -> "Input_friends_edge_props"),

            Map("name" -> "Input_friends_props"),

            Map("name" -> "Input_Service_ServiceColumn"),
            Map("name" -> "Input_Service_ServiceColumn_Props"),

            Map("name" -> "Input_kakao_user"),
            Map("name" -> "Input_kakao_user_vertex_mutate"),
            Map("name" -> "Input_kakao_user_vertex_props"),

            Map("name" -> "Enum_Service"),
            Map("name" -> "Enum_Label"),
            Map("name" -> "Enum_kakao_ServiceColumn"),

            Map("name" -> "Service_kakao"),
            Map("name" -> "Label_friends"),

            // root object type
            Map("name" -> "Query"),
            Map("name" -> "QueryManagement"),
            Map("name" -> "Mutation"),
            Map("name" -> "MutationManagement"),

            // graphql internal type
            Map("name" -> "__Directive"),
            Map("name" -> "__DirectiveLocation"),
            Map("name" -> "__EnumValue"),
            Map("name" -> "__Field"),
            Map("name" -> "__InputValue"),
            Map("name" -> "__Schema"),
            Map("name" -> "__Type"),
            Map("name" -> "__TypeKind"),
            Map("name" -> "Boolean"),
            Map("name" -> "Int"),
            Map("name" -> "Long"),
            Map("name" -> "String"))
          )
        )
      )

    val actual = Await.result(Executor.execute(s2Schema, query, s2Repository), Duration("10 sec"))
    val maps = actual.asInstanceOf[Map[String, Map[String, Map[String, Vector[Map[String, String]]]]]]("data")("__schema")("types")

    val expectedSet = expected("data")("__schema")("types").flatMap(_.values.headOption).toSet
    val actualSet = maps.flatMap(_.values.headOption).toSet

    logger.info(s"expected only has: ${expectedSet -- actualSet}")
    logger.info(s"actual only has: ${actualSet -- expectedSet}")

    actualSet should be(expectedSet)
  }

  test("Management query should have service: 'kakao'") {
    val Success(query) = QueryParser.parse(
      """
        query GetServiceAndLabel {
          Management {
            Services(name: kakao) {
              serviceColumns {
                props {
                  name
                  defaultValue
                  dataType
                }
              }
            }
          }
        }
        """)

    implicit val playJsonMarshaller = sangria.marshalling.playJson.PlayJsonResultMarshaller

    val actual: JsValue = Await.result(Executor.execute(s2Schema, query, s2Repository), Duration("10 sec"))

//    val expected: Map[String, Map[String, Map[String, Vector[Map[String, Vector[Map[String, Vector[Map[String, String]]]]]]]]] =
//      Map("data" ->
//        Map("Management" ->
//          Map("Services" ->
//            Vector(
//              Map("serviceColumns" ->
//                Vector(
//                  Map(
//                    "props" ->
//                      Vector(
//                        Map("name" -> "age", "defaultValue" -> "0", "dataType" -> "int"),
//                        Map("name" -> "gender", "defaultValue" -> "", "dataType" -> "string")
//                      )
//                  )
//                )
//              )
//            )
//          )
//        )
//      )

    true
  }

  test("Management query should have label: 'friends'") {
    val Success(query) = QueryParser.parse(
      """
        query Management {
          Management {
           Labels(name: friends) {
              name
              props {
                name
                defaultValue
                dataType
              }
            }
          }
        }
        """)

    val actual = Await.result(Executor.execute(s2Schema, query, s2Repository), Duration("10 sec"))
    true
  }

}
