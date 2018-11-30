package org.apache.s2graph.http

import java.nio.charset.Charset

import akka.http.scaladsl.marshalling.{Marshaller, ToEntityMarshaller}
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, Unmarshaller}
import akka.util.ByteString
import sangria.ast.Document
import sangria.parser.QueryParser
import sangria.renderer.{QueryRenderer, QueryRendererConfig}

trait SangriaGraphQLSupport {
  private val mediaTypes: Seq[MediaType.WithFixedCharset] =
    Seq(MediaType.applicationWithFixedCharset("graphql", HttpCharsets.`UTF-8`, "graphql"))

  private val unmarshallerContentTypes: Seq[ContentTypeRange] = mediaTypes.map(ContentTypeRange.apply)

  implicit def documentMarshaller(implicit config: QueryRendererConfig = QueryRenderer.Compact): ToEntityMarshaller[Document] = {
    Marshaller.oneOf(mediaTypes: _*) {
      mediaType ⇒
        Marshaller.withFixedContentType(ContentType(mediaType)) {
          json ⇒ HttpEntity(mediaType, QueryRenderer.render(json, config))
        }
    }
  }

  implicit val documentUnmarshaller: FromEntityUnmarshaller[Document] = {
    Unmarshaller.byteStringUnmarshaller
      .forContentTypes(unmarshallerContentTypes: _*)
      .map {
        case ByteString.empty ⇒ throw Unmarshaller.NoContentException
        case data ⇒
          import sangria.parser.DeliveryScheme.Throw
          QueryParser.parse(data.decodeString(Charset.forName("UTF-8")))
      }
  }
}
