package s2.config

import com.typesafe.config.{Config, ConfigFactory}

/**
 * Created by alec on 15. 3. 4..
 */

/**
 * phase에 따라 phase.conf 파일을 load 해주는 config factory
 */
object S2ConfigFactory {
  lazy val config: Config = _load

  @deprecated("do not call explicitly. use config", "0.0.6")
  def load(): Config = {
    _load
  }

  def _load: Config = {
    // default configuration file name : application.conf
    val sysConfig = ConfigFactory.parseProperties(System.getProperties)

    lazy val phase = if (!sysConfig.hasPath("phase")) "alpha" else sysConfig.getString("phase")
    sysConfig.withFallback(ConfigFactory.parseResourcesAnySyntax(s"$phase.conf")).withFallback(ConfigFactory.load())
  }
}
