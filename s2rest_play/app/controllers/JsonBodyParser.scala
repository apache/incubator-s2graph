package controllers

import com.kakao.s2graph.core.utils.logger
import play.api.Play
import play.api.libs.iteratee.{Execution, Iteratee}
import play.api.libs.json.{JsValue, Json}
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

    Traversable.takeUpTo[Array[Byte]](maxLength)
      .transform(Iteratee.consume[Array[Byte]]().map(c => new String(c, "UTF-8")))
      .flatMap(Iteratee.eofOrElse(Results.EntityTooLarge))
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

      val bodyParser: Iteratee[Array[Byte], Either[Result, Either[Future[Result], A]]] =
        Traversable.takeUpTo[Array[Byte]](maxLength).transform(
          Iteratee.consume[Array[Byte]]().map { bytes =>
            allCatch[A].either {
              parser(request, bytes)
            }.left.map {
              case NonFatal(e) =>
                val txt = new String(bytes)
                logger.error(s"$errorMessage: $txt", e)
                createBadResult(s"$errorMessage: $e")(request)
              case t => throw t
            }
          }
        ).flatMap(Iteratee.eofOrElse(Results.EntityTooLarge))

      bodyParser.mapM {
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
