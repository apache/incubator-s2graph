package com.kakao.ml.util

import org.scalatest.{Matchers, FunSuite}

class JsonTest extends FunSuite with Matchers {

  case class Person(firstName: String, lastName: String, age: Int, height: Double) {

    override def equals(obj: Any): Boolean = {
      obj match {
        case other: Person =>
          other.firstName == firstName && other.lastName == lastName && other.age == age && other.height === height +- 0.002
        case _ => super.equals(obj)
      }
    }
  }

  test("working") {
    val obj = Map[String, Any]("firstName" -> "graph", "lastName" -> "s2", "age" -> 2, "height" -> 2.0)
    val str = Json.toJsonString(obj)
    val pretty = Json.toPrettyJsonString(obj)
    val fromMap = Json.extract[Person](obj)
    val fromStr = Json.extract[Person](str)
    val fromPretty = Json.extract[Person](pretty)
    fromMap === fromStr
    fromMap === fromPretty
  }

}
