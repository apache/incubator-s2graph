package org.apache.s2graph.http

import java.nio.charset.Charset

import akka.http.scaladsl.marshalling.{Marshaller, ToEntityMarshaller}
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, Unmarshaller}
import akka.util.ByteString
import play.api.libs.json._

trait PlayJsonSupport {

  private val mediaTypes: Seq[MediaType.WithFixedCharset] =
    Seq(MediaType.applicationWithFixedCharset("json", HttpCharsets.`UTF-8`, "js"))

  private val unmarshallerContentTypes: Seq[ContentTypeRange] = mediaTypes.map(ContentTypeRange.apply)

  implicit val playJsonMarshaller: ToEntityMarshaller[JsValue] = {
    Marshaller.oneOf(mediaTypes: _*) { mediaType =>
      Marshaller.withFixedContentType(ContentType(mediaType)) {
        json => HttpEntity(mediaType, json.toString)
      }
    }
  }

  implicit val playJsonUnmarshaller: FromEntityUnmarshaller[JsValue] = {
    Unmarshaller.byteStringUnmarshaller
      .forContentTypes(unmarshallerContentTypes: _*)
      .map {
        case ByteString.empty => throw Unmarshaller.NoContentException
        case data => Json.parse(data.decodeString(Charset.forName("UTF-8")))
      }
  }

  trait ToPlayJson[T] {
    def toJson(msg: T): JsValue
  }

  import scala.language.reflectiveCalls

  object ToPlayJson {
    type ToPlayJsonReflective = {
      def toJson: JsValue
    }

    implicit def forToJson[A <: ToPlayJsonReflective] = new ToPlayJson[A] {
      def toJson(js: A) = js.toJson
    }

    implicit def forPlayJson[A <: JsValue] = new ToPlayJson[A] {
      def toJson(js: A) = js
    }
  }

  implicit object JsErrorJsonWriter extends Writes[JsError] {
    def writes(o: JsError): JsValue = Json.obj(
      "errors" -> JsArray(
        o.errors.map {
          case (path, validationErrors) => Json.obj(
            "path" -> Json.toJson(path.toString()),
            "validationErrors" -> JsArray(validationErrors.map(validationError => Json.obj(
              "message" -> JsString(validationError.message),
              "args" -> JsArray(validationError.args.map {
                case x: Int => JsNumber(x)
                case x => JsString(x.toString)
              })
            )))
          )
        }
      )
    )
  }

}
