package org.apache.s2graph.core.storage.hbase

import com.typesafe.config.ConfigFactory
import org.hbase.async.{GetRequest, PutRequest}
import org.scalatest.{Matchers, FunSuite}
import scala.collection.JavaConversions._

class AsynchbaseStorageTest extends FunSuite with Matchers {

  /** need secured cluster */
//  test("test secure cluster connection") {
//    val config = ConfigFactory.parseMap(
//      Map(
//        "hbase.zookeeper.quorum" -> "localhost",
//        "hbase.security.auth.enable" -> "true",
//        "hbase.security.authentication" -> "kerberos",
//        "hbase.kerberos.regionserver.principal" -> "hbase/_HOST@LOCAL.HBASE",
//        "hbase.sasl.clientconfig" -> "Client",
//        "java.security.krb5.conf" -> "krb5.conf",
//        "java.security.auth.login.config" -> "async-client.jaas.conf")
//    )
//
//    val client = AsynchbaseStorage.makeClient(config)
//    val table = "test".getBytes()
//
//    val putRequest = new PutRequest(table, "a".getBytes(), "e".getBytes, "a".getBytes, "a".getBytes)
//    val getRequest = new GetRequest(table, "a".getBytes(), "e".getBytes)
//    val ret = client.put(putRequest).join()
//    val kvs = client.get(getRequest).join()
//    for {
//      kv <- kvs
//    } {
//      println(kv.toString)
//    }
//  }
}