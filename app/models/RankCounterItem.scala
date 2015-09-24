package models

import play.api.libs.json.{Json, Writes}

/**
 * Created by alec on 15. 4. 15..
 */
case class RankCounterItem(rank: Int, id: String, score: Double)

case class RankCounterDimensionItem(interval: String, ts: Long, dimension: String, total: Double, ranks: Seq[RankCounterItem])

case class RankCounterResultMeta(service: String, action: String)

case class RankCounterResult(meta: RankCounterResultMeta, data: Seq[RankCounterDimensionItem])

object RankCounterItem {
  implicit val format = Json.format[RankCounterItem]
}

object RankCounterDimensionItem {
  implicit val writes = new Writes[RankCounterDimensionItem] {
    def writes(item: RankCounterDimensionItem) = Json.obj(
      "interval" -> item.interval,
      "ts" -> item.ts,
      "time" -> tsFormat.format(item.ts),
      "dimension" -> item.dimension,
      "total" -> item.total,
      "ranks" -> item.ranks
    )
  }
  implicit val reads = Json.reads[RankCounterDimensionItem]
}

object RankCounterResultMeta {
  implicit val format = Json.format[RankCounterResultMeta]
}

object RankCounterResult {
  implicit val format = Json.format[RankCounterResult]
}
