package s2.counter.core

import org.slf4j.LoggerFactory
import play.api.libs.json._
import s2.util.UnitConverter

import scala.util.{Failure, Success, Try}

/**
*  Created by hsleep(honeysleep@gmail.com) on 15. 10. 6..
*/
case class CounterEtlItem(ts: Long, service: String, action: String, item: String, dimension: JsValue, property: JsValue, useProfile: Boolean = false) {
  def toKafkaMessage: String = {
    s"$ts\t$service\t$action\t$item\t${dimension.toString()}\t${property.toString()}"
  }

  lazy val value = {
    property \ "value" match {
      case JsNumber(n) => n.longValue()
      case JsString(s) => s.toLong
      case _: JsUndefined => 1L
      case _ => throw new Exception("wrong type")
    }
  }
}

object CounterEtlItem {
  val log = LoggerFactory.getLogger(this.getClass)

  def apply(line: String): Option[CounterEtlItem] = {
    Try {
      val Array(ts, service, action, item, dimension, property) = line.split('\t')
      CounterEtlItem(UnitConverter.toMillis(ts.toLong), service, action, item, Json.parse(dimension), Json.parse(property))
    } match {
      case Success(item) =>
        Some(item)
      case Failure(ex) =>
        log.error(">>> failed")
        log.error(s"${ex.toString}: $line")
        None
    }
  }
}
