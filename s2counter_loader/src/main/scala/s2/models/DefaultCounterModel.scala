package s2.models

import s2.config.S2ConfigFactory

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 6. 8..
 */
case object DefaultCounterModel extends CounterModel(S2ConfigFactory.config)
