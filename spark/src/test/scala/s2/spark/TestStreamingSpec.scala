package s2.spark

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAfterAll

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 6. 17..
 */
class TestStreamingSpec extends Specification with BeforeAfterAll {
  private val master = "local[2]"
  private val appName = "test_streaming"
  private val batchDuration = Seconds(1)

  private var sc: SparkContext = _
  private var ssc: StreamingContext = _

  override def beforeAll(): Unit = {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)

    ssc = new StreamingContext(conf, batchDuration)

    sc = ssc.sparkContext
  }

  override def afterAll(): Unit = {
    if (ssc != null) {
      ssc.stop()
    }
  }
}
