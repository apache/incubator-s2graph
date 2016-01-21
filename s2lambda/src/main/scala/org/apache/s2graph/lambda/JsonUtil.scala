package org.apache.s2graph.lambda

import org.json4s.Extraction
import org.json4s.JsonAST.{JObject, JValue}
import org.json4s.native.JsonMethods

import scala.reflect.ClassTag

object JsonUtil {

  val emptyJsonValue = JObject()

  type JVal = JValue

  implicit val formats = org.json4s.DefaultFormats

  def toPrettyJsonString(obj: Any): String = JsonMethods.pretty(JsonMethods.render(Extraction.decompose(obj)))

  def toJsonString(obj: Any): String = JsonMethods.compact(JsonMethods.render(Extraction.decompose(obj)))

  def extract[T](src: Any)(implicit ct: ClassTag[T]): T = {
    src match {
      case s: String => JsonMethods.parse(s).extract[T](formats, Manifest.classType(ct.runtimeClass))
      case j: JVal => j.extract[T](formats, Manifest.classType(ct.runtimeClass))
      case barr: Array[Byte] =>
        val s = new String(barr, "UTF-8")
        JsonMethods.parse(s).extract[T](formats, Manifest.classType(ct.runtimeClass))
      case any => Extraction.decompose(any).extract[T](formats, Manifest.classType(ct.runtimeClass))
    }
  }


}
