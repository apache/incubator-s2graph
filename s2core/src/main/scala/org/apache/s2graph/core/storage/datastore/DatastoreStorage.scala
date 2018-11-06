//package org.apache.s2graph.core.storage.datastore
//
//import com.google.cloud.datastore._
//import com.typesafe.config.Config
//import org.apache.s2graph.core._
//
//object DatastoreStorage {
//  def initDatasatore(config: Config): Datastore = {
//    DatastoreOptions.getDefaultInstance.getService
//  }
//
//}
//class DatastoreStorage(graph: S2GraphLike, config: Config) {
//  import DatastoreStorage._
//  val datastore: Datastore = initDatasatore(config)
//}
