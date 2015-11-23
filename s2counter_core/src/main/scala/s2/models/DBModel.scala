package s2.models

import com.typesafe.config.Config
import s2.config.S2CounterConfig
import scalikejdbc._

/**
 * Created by alec on 15. 3. 31..
 */
object DBModel {
  private var initialized = false
  
  def initialize(config: Config): Unit = {
    if (!initialized) {
      this synchronized {
        if (!initialized) {
          val s2Config = new S2CounterConfig(config)
          Class.forName(s2Config.DB_DEFAULT_DRIVER)
          val settings = ConnectionPoolSettings(initialSize = 0, maxSize = 10, connectionTimeoutMillis = 5000L, validationQuery = "select 1;")

          ConnectionPool.singleton(s2Config.DB_DEFAULT_URL, s2Config.DB_DEFAULT_USER, s2Config.DB_DEFAULT_PASSWORD, settings)
          initialized = true
        }
      }
    }
  }
}
