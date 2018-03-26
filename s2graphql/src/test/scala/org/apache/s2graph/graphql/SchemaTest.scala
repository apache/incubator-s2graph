package org.apache.s2graph.graphql

import com.typesafe.config.{ConfigFactory}
import org.apache.s2graph.core.utils.logger
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import sangria.execution.Executor
import sangria.parser.QueryParser


import scala.util._

class SchemaTest extends FunSuite with Matchers with BeforeAndAfterAll {
  var testGraph: TestGraph = _

  override def beforeAll = {
    val config = ConfigFactory.load()
    testGraph = new BasicGraph(config)

    logger.info("-" * 80)
    println(testGraph.showSchema)
    logger.info("-" * 80)
  }

  override def afterAll(): Unit = {
    testGraph.cleanup()
  }

  test("s2 schema should has types") {
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
            Map("name" -> "Input_Column"),
            Map("name" -> "Input_Column_Props"),

            Map("name" -> "ColumnMeta"),
            Map("name" -> "LabelMeta"),

            Map("name" -> "Label"),
            Map("name" -> "LabelIndex"),

            Map("name" -> "Service"),
            Map("name" -> "ServiceColumn"),

            Map("name" -> "Input_Service_ServiceColumn"),
            Map("name" -> "Input_Service_ServiceColumn_Props"),
            Map("name" -> "Input_label_friends_param"),
            Map("name" -> "Input_vertex_kakao_param"),

            Map("name" -> "Input_kakao_user"),
            Map("name" -> "Input_kakao_user_vertex_mutate"),

            Map("name" -> "Enum_Service"),
            Map("name" -> "Enum_Label"),
            Map("name" -> "Enum_kakao_ServiceColumn"),

            Map("name" -> "Service_kakao"),

            Map("name" -> "Label_friends"),
            Map("name" -> "Label_friends_from"),
            Map("name" -> "Label_friends_to"),

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

    val actual = Await.result(Executor.execute(testGraph.schema, query, testGraph.repository), Duration("10 sec"))
    val maps = actual.asInstanceOf[Map[String, Map[String, Map[String, Vector[Map[String, String]]]]]]("data")("__schema")("types")

    val expectedSet = expected("data")("__schema")("types").flatMap(_.values.headOption).toSet
    val actualSet = maps.flatMap(_.values.headOption).toSet

    logger.info(s"expected only has: ${expectedSet -- actualSet}")
    logger.info(s"actual only has: ${actualSet -- expectedSet}")

    actualSet.toList.sorted shouldBe expectedSet.toList.sorted
  }
}
