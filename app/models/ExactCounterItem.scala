package models

import play.api.libs.json.{Json, Writes}

/**
 * Created by alec on 15. 4. 15..
 */
case class ExactCounterItem(ts: Long, count: Long, score: Double)

case class ExactCounterIntervalItem(interval: String, dimension: Map[String, String], counter: Seq[ExactCounterItem])

case class ExactCounterResultMeta(service: String, action: String, item: String)

case class ExactCounterResult(meta: ExactCounterResultMeta, data: Seq[ExactCounterIntervalItem])

object ExactCounterItem {
  implicit val writes = new Writes[ExactCounterItem] {
    def writes(item: ExactCounterItem) = Json.obj(
      "ts" -> item.ts,
      "time" -> tsFormat.format(item.ts),
      "count" -> item.count,
      "score" -> item.score
    )
  }
  implicit val reads = Json.reads[ExactCounterItem]
}

object ExactCounterIntervalItem {
  implicit val format = Json.format[ExactCounterIntervalItem]
}

object ExactCounterResultMeta {
  implicit val format = Json.format[ExactCounterResultMeta]
}

object ExactCounterResult {
  implicit val formats = Json.format[ExactCounterResult]
}
