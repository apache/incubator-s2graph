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

package org.apache.s2graph.rest.play.controllers

import akka.util.ByteString
import org.apache.s2graph.core.utils.logger
import play.api.Play
import play.api.libs.iteratee.Iteratee
import play.api.libs.json.{JsValue, Json}
import play.api.libs.streams.Streams
import play.api.mvc._

import scala.concurrent.Future
import scala.util.control.NonFatal

object s2parse extends BodyParsers {

  import parse._

  val defaultMaxTextLength = 1024 * 512
  val defaultMaxJsonLength = 1024 * 512

  def json: BodyParser[JsValue] = json(defaultMaxTextLength)

  /**
    * parseText with application/json header for Pre-Process text
    */
  def jsonText: BodyParser[String] = when(
    _.contentType.exists(m => m.equalsIgnoreCase("text/json") || m.equalsIgnoreCase("application/json")),
    jsonText(defaultMaxTextLength),
    createBadResult("Expecting text/json or application/json body")
  )

  private def jsonText(maxLength: Int): BodyParser[String] = BodyParser("json, maxLength=" + maxLength) { request =>
    import play.api.libs.iteratee.Execution.Implicits.trampoline
    import play.api.libs.iteratee.Traversable

    val iteratee = Traversable.takeUpTo[ByteString](maxLength)
      .transform(Iteratee.consume[ByteString]().map(_.utf8String))
      .flatMap(Iteratee.eofOrElse(Results.EntityTooLarge))

    Streams.iterateeToAccumulator(iteratee)
  }

  def json(maxLength: Int): BodyParser[JsValue] = when(
    _.contentType.exists(m => m.equalsIgnoreCase("text/json") || m.equalsIgnoreCase("application/json")),
    tolerantJson(maxLength),
    createBadResult("Expecting text/json or application/json body")
  )

  def tolerantJson(maxLength: Int): BodyParser[JsValue] =
    tolerantBodyParser[JsValue]("json", maxLength, "Invalid Json") { (request, bytes) =>
      // Encoding notes: RFC 4627 requires that JSON be encoded in Unicode, and states that whether that's
      // UTF-8, UTF-16 or UTF-32 can be auto detected by reading the first two bytes. So we ignore the declared
      // charset and don't decode, we passing the byte array as is because Jackson supports auto detection.
      Json.parse(bytes)
    }

  private def tolerantBodyParser[A](name: String, maxLength: Int, errorMessage: String)(parser: (RequestHeader, Array[Byte]) => A): BodyParser[A] =
    BodyParser(name + ", maxLength=" + maxLength) { request =>
      import play.api.libs.iteratee.Execution.Implicits.trampoline
      import play.api.libs.iteratee.Traversable

      import scala.util.control.Exception._

      val bodyParser: Iteratee[ByteString, Either[Result, Either[Future[Result], A]]] =
        Traversable.takeUpTo[ByteString](maxLength).transform(
          Iteratee.consume[ByteString]().map { bytes =>
            allCatch[A].either {
              parser(request, bytes.toByteBuffer.array())
            }.left.map {
              case NonFatal(e) =>
                val txt = bytes.utf8String
                logger.error(s"$errorMessage: $txt", e)
                createBadResult(s"$errorMessage: $e")(request)
              case t => throw t
            }
          }
        ).flatMap(Iteratee.eofOrElse(Results.EntityTooLarge))

      Streams.iterateeToAccumulator(bodyParser).mapFuture {
        case Left(tooLarge) => Future.successful(Left(tooLarge))
        case Right(Left(badResult)) => badResult.map(Left.apply)
        case Right(Right(body)) => Future.successful(Right(body))
      }
    }

  private def createBadResult(msg: String): RequestHeader => Future[Result] = { request =>
    Play.maybeApplication.map(_.global.onBadRequest(request, msg))
      .getOrElse(Future.successful(Results.BadRequest))
  }
}
