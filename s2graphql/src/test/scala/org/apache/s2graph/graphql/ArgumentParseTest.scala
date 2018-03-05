package org.apache.s2graph.graphql

import org.scalatest.{FunSuite, Matchers}
import play.api.libs.json.{JsObject, JsString, Json}
import sangria.ast.Document

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import sangria.macros._
import sangria.execution.Executor
import sangria.execution.deferred.DeferredResolver

class ArgumentParseTest extends FunSuite with Matchers {
  test("parseAddPropsToServiceColumn") {
    true
  }
}
