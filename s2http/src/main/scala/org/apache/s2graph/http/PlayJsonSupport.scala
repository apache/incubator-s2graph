package org.apache.s2graph.http

import java.nio.charset.Charset

import akka.http.scaladsl.marshalling.{Marshaller, ToEntityMarshaller}
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, Unmarshaller}
import akka.util.ByteString
import play.api.libs.json.{JsValue, Json}

trait PlayJsonSupport {

  val mediaTypes: Seq[MediaType.WithFixedCharset] =
    Seq(MediaType.applicationWithFixedCharset("json", HttpCharsets.`UTF-8`, "js"))

  val unmarshallerContentTypes: Seq[ContentTypeRange] = mediaTypes.map(ContentTypeRange.apply)

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
}
