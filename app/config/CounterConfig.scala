package config

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 9. 3..
 */
object CounterConfig {
  // kafka
  lazy val KAFKA_TOPIC_COUNTER = s"s2counter-${Config.PHASE}"
  lazy val KAFKA_TOPIC_COUNTER_TRX = s"s2counter-trx-${Config.PHASE}"
}
