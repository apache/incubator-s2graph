package subscriber

import com.kakao.s2graph.core.Management
import com.kakao.s2graph.core.types.HBaseType
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._
import subscriber.TransferToHFile._

/**
  * Created by Eric on 2015. 12. 2..
  */
class TransferToHFileTest  extends FlatSpec with BeforeAndAfterAll with Matchers {

  private val master = "local[2]"
  private val appName = "example-spark"

  private var sc: SparkContext = _

  val dataWithoutDir =
    """
      |1447686000000	insertBulk	e	a	b	friends_rel	{}
      |1447686000000	insertBulk	e	a	c	friends_rel	{}
      |1447686000000	insertBulk	e	a	d	friends_rel	{}
      |1447686000000	insertBulk	e	b	d	friends_rel	{}
      |1447686000000	insertBulk	e	b	e	friends_rel	{}
    """.stripMargin.trim

  val dataWithDir =
    """
     |1447686000000	insertBulk	e	a	b	friends_rel	{}	out
     |1447686000000	insertBulk	e	b	a	friends_rel	{}	in
     |1447686000000	insertBulk	e	a	c	friends_rel	{}	out
     |1447686000000	insertBulk	e	c	a	friends_rel	{}	in
     |1447686000000	insertBulk	e	a	d	friends_rel	{}	out
     |1447686000000	insertBulk	e	d	a	friends_rel	{}	in
     |1447686000000	insertBulk	e	b	d	friends_rel	{}	out
     |1447686000000	insertBulk	e	d	b	friends_rel	{}	in
     |1447686000000	insertBulk	e	b	e	friends_rel	{}	out
     |1447686000000	insertBulk	e	e	b	friends_rel	{}	in
    """.stripMargin.trim

  override def beforeAll(): Unit = {
    println("### beforeAll")

    GraphSubscriberHelper.apply("dev", "none", "none", "none")
    // 1. create service
    if(Management.findService("loader-test").isEmpty) {
      println(">>> create service...")
      Management.createService("loader-test", "localhost", "loader-test-dev", 1, None, "gz")
    }

    // 2. create label
    if(Management.findLabel("friends_rel").isEmpty) {
      println(">>> create label...")
      Management.createLabel(
        "friends_rel",
        "loader-test", "user_id", "string",
        "loader-test", "user_id", "string",
        true,
        "loader-test",
        Seq(),
        Seq(),
        "weak",
        None, None,
        HBaseType.DEFAULT_VERSION,
        false,
        Management.defaultCompressionAlgorithm
      )
    }

    // create spark context
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)

    sc = new SparkContext(conf)
  }

  override def afterAll(): Unit = {
    println("### afterALL")
    if (sc != null) {
      sc.stop()
    }

    Management.deleteLabel("friends_rel")
  }

  "buildDegreePutRequest" should "transform degree to PutRequest" in {
    val putReqs = buildDegreePutRequests("a", "friends_rel", "out", 3L)
    putReqs.size should equal(1)
  }

  "toKeyValues" should "transform edges to KeyValues on edge format data without direction" in {
    val rdd = sc.parallelize(dataWithoutDir.split("\n"))

    val kvs = rdd.mapPartitions { iter =>
      GraphSubscriberHelper.apply("dev", "none", "none", "none")
      TransferToHFile.toKeyValues(iter.toSeq, Map.empty[String, String], false)
    }
    kvs.foreach(println)
    // edges * 2 (snapshot edges + indexed edges)
    kvs.count() should equal(10)


    val kvsAutoCreated = rdd.mapPartitions { iter =>
      GraphSubscriberHelper.apply("dev", "none", "none", "none")
      TransferToHFile.toKeyValues(iter.toSeq, Map.empty[String, String], true)
    }

    // edges * 3 (snapshot edges + indexed edges + reverse edges)
    kvsAutoCreated.count() should equal(15)
  }

  "toKeyValues" should "transform edges to KeyValues on edge format data with direction" in {
    val rdd = sc.parallelize(dataWithDir.split("\n"))

    val kvs = rdd.mapPartitions { iter =>
      GraphSubscriberHelper.apply("dev", "none", "none", "none")
      TransferToHFile.toKeyValues(iter.toSeq, Map.empty[String, String], false)
    }

    // edges * 2 (snapshot edges + indexed edges)
    kvs.count() should equal(20)
  }

  "buildDegrees" should "build degrees on edge format data without direction" in {
    val rdd = sc.parallelize(dataWithoutDir.split("\n"))

    // autoCreate = false
    val degrees = TransferToHFile.buildDegrees(rdd, Map.empty[String, String], false).reduceByKey { (agg, current) =>
      agg + current
    }.collectAsMap()
    degrees.size should equal(2)

    degrees should contain(DegreeKey("a", "friends_rel", "out") -> 3L)
    degrees should contain(DegreeKey("b", "friends_rel", "out") -> 2L)


    // autoCreate = true
    val degreesAutoCreated = TransferToHFile.buildDegrees(rdd, Map.empty[String, String], true).reduceByKey { (agg, current) =>
      agg + current
    }.collectAsMap()
    degreesAutoCreated.size should equal(6)

    degreesAutoCreated should contain(DegreeKey("a", "friends_rel", "out") -> 3L)
    degreesAutoCreated should contain(DegreeKey("b", "friends_rel", "out") -> 2L)
    degreesAutoCreated should contain(DegreeKey("b", "friends_rel", "in") -> 1L)
    degreesAutoCreated should contain(DegreeKey("c", "friends_rel", "in") -> 1L)
    degreesAutoCreated should contain(DegreeKey("d", "friends_rel", "in") -> 2L)
    degreesAutoCreated should contain(DegreeKey("e", "friends_rel", "in") -> 1L)
  }

  "buildDegrees" should "build degrees on edge format data with direction" in {
    val rdd = sc.parallelize(dataWithDir.split("\n"))

    val degrees = TransferToHFile.buildDegrees(rdd, Map.empty[String, String], false).reduceByKey { (agg, current) =>
      agg + current
    }.collectAsMap()

    degrees.size should equal(6)

    degrees should contain(DegreeKey("a", "friends_rel", "out") -> 3L)
    degrees should contain(DegreeKey("b", "friends_rel", "out") -> 2L)
    degrees should contain(DegreeKey("b", "friends_rel", "in") -> 1L)
    degrees should contain(DegreeKey("c", "friends_rel", "in") -> 1L)
    degrees should contain(DegreeKey("d", "friends_rel", "in") -> 2L)
    degrees should contain(DegreeKey("e", "friends_rel", "in") -> 1L)
  }
}
