/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.s2graph.core

import java.util
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.concurrent.{Executors, TimeUnit}

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.configuration.{BaseConfiguration, Configuration}
import org.apache.s2graph.core.index.IndexProvider
import org.apache.s2graph.core.io.tinkerpop.optimize.S2GraphStepStrategy
import org.apache.s2graph.core.mysqls._
import org.apache.s2graph.core.storage.hbase.AsynchbaseStorage
import org.apache.s2graph.core.storage.{MutateResponse, Storage}
import org.apache.s2graph.core.types._
import org.apache.s2graph.core.utils.{DeferCache, Extensions, logger}
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies
import org.apache.tinkerpop.gremlin.structure.{Direction, Edge, Graph}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent._
import scala.concurrent.duration.Duration
import scala.util.Try


object S2Graph {

  type HashKey = (Int, Int, Int, Int, Boolean)
  type FilterHashKey = (Int, Int)

  val DefaultScore = 1.0
  val FetchAllLimit = 10000000
  val DefaultFetchLimit = 1000

  private val DefaultConfigs: Map[String, AnyRef] = Map(
    "hbase.zookeeper.quorum" -> "localhost",
    "hbase.table.name" -> "s2graph",
    "hbase.table.compression.algorithm" -> "gz",
    "phase" -> "dev",
    "db.default.driver" ->  "org.h2.Driver",
    "db.default.url" -> "jdbc:h2:file:./var/metastore;MODE=MYSQL",
    "db.default.password" -> "graph",
    "db.default.user" -> "graph",
    "cache.max.size" -> java.lang.Integer.valueOf(0),
    "cache.ttl.seconds" -> java.lang.Integer.valueOf(-1),
    "hbase.client.retries.number" -> java.lang.Integer.valueOf(20),
    "hbase.rpcs.buffered_flush_interval" -> java.lang.Short.valueOf(100.toShort),
    "hbase.rpc.timeout" -> java.lang.Integer.valueOf(600000),
    "max.retry.number" -> java.lang.Integer.valueOf(100),
    "lock.expire.time" -> java.lang.Integer.valueOf(1000 * 60 * 10),
    "max.back.off" -> java.lang.Integer.valueOf(100),
    "back.off.timeout" -> java.lang.Integer.valueOf(1000),
    "hbase.fail.prob" -> java.lang.Double.valueOf(-0.1),
    "delete.all.fetch.size" -> java.lang.Integer.valueOf(1000),
    "delete.all.fetch.count" -> java.lang.Integer.valueOf(200),
    "future.cache.max.size" -> java.lang.Integer.valueOf(100000),
    "future.cache.expire.after.write" -> java.lang.Integer.valueOf(10000),
    "future.cache.expire.after.access" -> java.lang.Integer.valueOf(5000),
    "future.cache.metric.interval" -> java.lang.Integer.valueOf(60000),
    "query.future.cache.max.size" -> java.lang.Integer.valueOf(1000),
    "query.future.cache.expire.after.write" -> java.lang.Integer.valueOf(10000),
    "query.future.cache.expire.after.access" -> java.lang.Integer.valueOf(5000),
    "query.future.cache.metric.interval" -> java.lang.Integer.valueOf(60000),
    "s2graph.storage.backend" -> "hbase",
    "query.hardlimit" -> java.lang.Integer.valueOf(100000),
    "hbase.zookeeper.znode.parent" -> "/hbase",
    "query.log.sample.rate" -> Double.box(0.05)
  )

  var DefaultConfig: Config = ConfigFactory.parseMap(DefaultConfigs)
  val numOfThread = Runtime.getRuntime.availableProcessors()
  val threadPool = Executors.newFixedThreadPool(numOfThread)
  val ec = ExecutionContext.fromExecutor(threadPool)

  val DefaultServiceName = ""
  val DefaultColumnName = "vertex"
  val DefaultLabelName = "_s2graph"

  val graphStrategies: TraversalStrategies =
    TraversalStrategies.GlobalCache.getStrategies(classOf[Graph]).addStrategies(S2GraphStepStrategy.instance)

  def toTypeSafeConfig(configuration: Configuration): Config = {
    val m = new mutable.HashMap[String, AnyRef]()
    for {
      key <- configuration.getKeys
      value = configuration.getProperty(key)
    } {
      m.put(key, value)
    }
    val config  = ConfigFactory.parseMap(m).withFallback(DefaultConfig)
    config
  }

  def fromTypeSafeConfig(config: Config): Configuration = {
    val configuration = new BaseConfiguration()
    for {
      e <- config.entrySet()
    } {
      configuration.setProperty(e.getKey, e.getValue.unwrapped())
    }
    configuration
  }

  def open(configuration: Configuration): S2Graph = {
    new S2Graph(configuration)(ec)
  }

  def initStorage(graph: S2Graph, config: Config)(ec: ExecutionContext): Storage = {
    val storageBackend = config.getString("s2graph.storage.backend")
    logger.info(s"[InitStorage]: $storageBackend")

    storageBackend match {
      case "hbase" => new AsynchbaseStorage(graph, config)
      case _ => throw new RuntimeException("not supported storage.")
    }
  }

  def parseCacheConfig(config: Config, prefix: String): Config = {
    import scala.collection.JavaConversions._

    val kvs = new java.util.HashMap[String, AnyRef]()
    for {
      entry <- config.entrySet()
      (k, v) = (entry.getKey, entry.getValue) if k.startsWith(prefix)
    } yield {
      val newKey = k.replace(prefix, "")
      kvs.put(newKey, v.unwrapped())
    }
    ConfigFactory.parseMap(kvs)
  }
}


@Graph.OptIns(value = Array(
  new Graph.OptIn(value = Graph.OptIn.SUITE_PROCESS_STANDARD),
  new Graph.OptIn(value = Graph.OptIn.SUITE_STRUCTURE_STANDARD)
))
@Graph.OptOuts(value = Array(
  /* Process */
  /* branch: passed all. */
//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.BranchTest$Traversals", method = "*", reason = "no"),
// passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.ChooseTest$Traversals", method = "*", reason = "no"),
// passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.OptionalTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.LocalTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionTest$Traversals", method = "*", reason = "no"),
//  passed: all


  /* filter */
//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.AndTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.CoinTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.CyclicPathTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupTest$Traversals", method = "*", reason = "no"),
//  passed: all

  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.DropTest$Traversals", method = "g_V_properties_drop", reason = "please find bug on this case."),
//  passed: all, failed: g_V_properties_drop

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.IsTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.OrTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.SampleTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.SimplePathTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.TailTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest$Traversals", method = "*", reason = "no"),
//  passed: all,

  /* map */
//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.AddEdgeTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.CoalesceTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ConstantTest$Traversals", method = "*", reason = "no"),
//  passed: all

  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.CountTest$Traversals", method = "g_V_both_both_count", reason = "count differ very little. fix this."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.CountTest$Traversals", method = "g_V_repeatXoutX_timesX3X_count", reason = "count differ very little. fix this."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.CountTest$Traversals", method = "g_V_repeatXoutX_timesX8X_count", reason = "count differ very litter. fix this."),
//  passed: all, failed: g_V_both_both_count, g_V_repeatXoutX_timesX3X_count, g_V_repeatXoutX_timesX8X_count

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.FlatMapTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.FoldTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.LoopsTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MapTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MapKeysTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MapValuesTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest$CountMatchTraversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest$GreedyMatchTraversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MaxTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MeanTest$Traversals", method = "*", reason = "no"),
//  failed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MinTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SumTest$Traversals", method = "*", reason = "no"),
//  failed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.PathTest$Traversals", method = "*", reason = "no"),
//  passed: all

  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest$Traversals", method = "testProfileStrategyCallback", reason = "NullPointerException. fix this."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest$Traversals", method = "g_V_whereXinXcreatedX_count_isX1XX_name_profile", reason = "java.lang.AssertionError: There should be 3 top-level metrics. fix this."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest$Traversals", method = "g_V_whereXinXcreatedX_count_isX1XX_name_profileXmetricsX", reason = "expected 2, actual 6. fix this."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest$Traversals", method = "grateful_V_out_out_profileXmetricsX", reason = "expected 8049, actual 8046. fix this."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest$Traversals", method = "grateful_V_out_out_profile", reason = "expected 8049, actual 8046. fix this."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest$Traversals", method = "modern_V_out_out_profileXmetricsX", reason = "expected 2, actual 6. fix this."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest$Traversals", method = "modern_V_out_out_profile", reason = "expected 2, actual 6. fix this."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest$Traversals", method = "testProfileStrategyCallbackSideEffect", reason = "NullPointerException. fix this."),
//  failed: grateful_V_out_out_profileXmetricsX, g_V_repeat_both_profileXmetricsX, grateful_V_out_out_profile, g_V_repeat_both_profile

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ProjectTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.UnfoldTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ValueMapTest$Traversals", method = "*", reason = "no"),
//  passed: all

  /* sideEffect */
//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.AggregateTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTestV3d0$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupCountTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.InjectTest$Traversals", method = "*", reason = "no"),
//  passed: all

  //  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SackTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectCapTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectTest$Traversals", method = "*", reason = "no"),
//  passed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.StoreTest$Traversals", method = "*", reason = "no"),
//  passed: all

  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SubgraphTest$Traversals", method = "g_V_withSideEffectXsgX_repeatXbothEXcreatedX_subgraphXsgX_outVX_timesX5X_name_dedup", reason = "Expected 5, Actual 6."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SubgraphTest$Traversals", method = "g_V_withSideEffectXsgX_outEXknowsX_subgraphXsgX_name_capXsgX", reason = "Expected 3, Actual 6"),
//  passed: all, failed: g_V_withSideEffectXsgX_repeatXbothEXcreatedX_subgraphXsgX_outVX_timesX5X_name_dedup, g_V_withSideEffectXsgX_outEXknowsX_subgraphXsgX_name_capXsgX

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.TreeTest$Traversals", method = "*", reason = "no"),
//  passed: all


  /* compliance */
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.CoreTraversalTest", method = "shouldThrowExceptionWhenIdsMixed", reason = "VertexId is not Element."),
//  passed: all

  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.TraversalInterruptionTest", method = "*", reason = "not supported yet."),
//  failed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.TranslationStrategyProcessTest", method = "*", reason = "no"),
//  passed: all

  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ElementIdStrategyProcessTest", method = "shouldGenerateDefaultIdOnAddVWithSpecifiedId", reason = "GraphStep.processNextStart throw FastNoSuchElementException when GraphStep.start = true and GraphStep.end = true."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ElementIdStrategyProcessTest", method = "shouldGenerateDefaultIdOnAddVWithGeneratedCustomId", reason = "GraphStep.processNextStart throw FastNoSuchElementException when GraphStep.start = true and GraphStep.end = true."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ElementIdStrategyProcessTest", method = "shouldGenerateDefaultIdOnGraphAddVWithGeneratedDefaultId", reason = "GraphStep.processNextStart throw FastNoSuchElementException when GraphStep.start = true and GraphStep.end = true."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ElementIdStrategyProcessTest", method = "shouldGenerateDefaultIdOnAddVWithGeneratedDefaultId", reason = "GraphStep.processNextStart throw FastNoSuchElementException when GraphStep.start = true and GraphStep.end = true."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ElementIdStrategyProcessTest", method = "shouldGenerateDefaultIdOnGraphAddVWithGeneratedCustomId", reason = "GraphStep.processNextStart throw FastNoSuchElementException when GraphStep.start = true and GraphStep.end = true."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ElementIdStrategyProcessTest", method = "shouldGenerateDefaultIdOnGraphAddVWithSpecifiedId", reason = "GraphStep.processNextStart throw FastNoSuchElementException when GraphStep.start = true and GraphStep.end = true."),
//  failed: shouldGenerateDefaultIdOnAddVWithSpecifiedId, shouldGenerateDefaultIdOnAddVWithGeneratedCustomId, shouldGenerateDefaultIdOnGraphAddVWithGeneratedDefaultId,
//  shouldGenerateDefaultIdOnAddVWithGeneratedDefaultId, shouldGenerateDefaultIdOnGraphAddVWithGeneratedCustomId, shouldGenerateDefaultIdOnGraphAddVWithSpecifiedId

  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategyProcessTest", method = "*", reason = "not supported yet."),
//  failed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategyProcessTest", method = "*", reason = "no"),
//  passed: all

  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.PartitionStrategyProcessTest", method = "*", reason = "not supported yet."),
//  failed: all

//  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategyProcessTest", method = "*", reason = "no"),
//  passed: all

  /* Structure */
  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.EdgeTest$BasicEdgeTest", method="shouldValidateIdEquality", reason="reference equals on EdgeId is not supported."),
  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.EdgeTest$BasicEdgeTest", method="shouldValidateEquality", reason="reference equals on EdgeId is not supported."),
  // passed: all, failed: none

//  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.GraphConstructionTest", method="*", reason="no"),
  // passed: all, failed: none

//  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.PropertyTest", method="*", reason="no"),
  // passed: all, failed: none

//  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.VertexPropertyTest", method="*", reason="no"),
  // passed: all, failed: none

//  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.FeatureSupportTest", method="*", reason="no"),
  // passed: all, failed: none

  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.VertexTest$BasicVertexTest", method="shouldHaveExceptionConsistencyWhenAssigningSameIdOnEdge", reason="S2Vertex.addEdge behave as upsert."),
  // passed: , failed: shouldHaveExceptionConsistencyWhenAssigningSameIdOnEdge

  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdgeTest", method="shouldNotEvaluateToEqualDifferentId", reason="reference equals is not supported."),
  // passed: all, failed: shouldNotEvaluateToEqualDifferentId

//  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexTest", method="*", reason="no"),
  // passed: all, failed: none

//  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.util.detached.DetachedGraphTest", method="*", reason="no"),
  // passed: all, failed: none,  all ignored

  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.util.detached.DetachedPropertyTest", method="shouldNotBeEqualPropertiesAsThereIsDifferentKey", reason="reference equals is not supported."),
//  // passed: , failed: shouldNotBeEqualPropertiesAsThereIsDifferentKey

//  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexPropertyTest", method="*", reason="no"),
  // passed: all, failed: none

  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.GraphTest", method="shouldRemoveVertices", reason="random label creation is not supported. all label need to be pre-configured."),
  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.GraphTest", method="shouldHaveExceptionConsistencyWhenAssigningSameIdOnVertex", reason="Assigning the same ID to an Element update instead of throwing exception."),
  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.GraphTest", method="shouldRemoveEdges", reason="random label creation is not supported. all label need to be pre-configured."),
  // passed: , failed:

  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceEdgeTest", method="shouldNotEvaluateToEqualDifferentId", reason="Assigning the same ID to an Element update instead of throwing exception."),
  // passed: all, skip: shouldNotEvaluateToEqualDifferentId


//  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertexPropertyTest", method="*", reason="no"),
  // passed: all, failed: none

//  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceGraphTest", method="*", reason="no"),
  // passed: all, failed: none, all ignored
//  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertexTest", method="*", reason="no"),
  // passed: all, failed: none, all ignored

//  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.util.star.StarGraphTest", method="*", reason="no"),
  // passed: all,

  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.algorithm.generator.CommunityGeneratorTest$DifferentDistributionsTest", method="shouldGenerateDifferentGraph", specific="test(NormalDistribution{stdDeviation=2.0, mean=0.0},PowerLawDistribution{gamma=2.4, multiplier=0.0},0.1)", reason="graphson-v2-embedded is not supported."),
  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.algorithm.generator.CommunityGeneratorTest$DifferentDistributionsTest", method="shouldGenerateDifferentGraph", specific="test(NormalDistribution{stdDeviation=2.0, mean=0.0},PowerLawDistribution{gamma=2.4, multiplier=0.0},0.5)", reason="graphson-v2-embedded is not supported."),
  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.algorithm.generator.CommunityGeneratorTest$DifferentDistributionsTest", method="shouldGenerateDifferentGraph", specific="test(NormalDistribution{stdDeviation=2.0, mean=0.0},NormalDistribution{stdDeviation=4.0, mean=0.0},0.5)", reason="graphson-v2-embedded is not supported."),
  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.algorithm.generator.CommunityGeneratorTest$DifferentDistributionsTest", method="shouldGenerateDifferentGraph", specific="test(NormalDistribution{stdDeviation=2.0, mean=0.0},NormalDistribution{stdDeviation=4.0, mean=0.0},0.1)", reason="graphson-v2-embedded is not supported."),
  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.algorithm.generator.CommunityGeneratorTest$DifferentDistributionsTest", method="shouldGenerateDifferentGraph", specific="test(PowerLawDistribution{gamma=2.3, multiplier=0.0},PowerLawDistribution{gamma=2.4, multiplier=0.0},0.25)", reason="graphson-v2-embedded is not supported."),
  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.algorithm.generator.CommunityGeneratorTest$DifferentDistributionsTest", method="shouldGenerateDifferentGraph", specific="test(PowerLawDistribution{gamma=2.3, multiplier=0.0},NormalDistribution{stdDeviation=4.0, mean=0.0},0.25)", reason="graphson-v2-embedded is not supported."),
  // passed: all, except shouldGenerateDifferentGraph method.

  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.algorithm.generator.DistributionGeneratorTest", method="*", reason="non-deterministic test."),
  // all failed.

  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.SerializationTest$GryoTest", method="shouldSerializeTree", reason="order of children is reversed. not sure why."),
  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.SerializationTest$GraphSONTest", method="shouldSerializeTraversalMetrics", reason="expected 2, actual 3."),
  // passed: all, failed: $GryoTest.shouldSerializeTree

//  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.io.IoCustomTest", method="*", reason="no"),
  // all ignored.

//  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.io.IoPropertyTest", method="*", reason="no"),
  // all passed.

  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.io.IoVertexTest", method="shouldReadWriteVertexWithBOTHEdges", specific="graphson-v2-embedded", reason="Vertex.id() is deserialized as string, not class in graphson-v2-embedded."),
  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.io.IoVertexTest", method="shouldReadWriteVertexWithINEdges", specific="graphson-v2-embedded", reason="Vertex.id() is deserialized as string, not class in graphson-v2-embedded."),
  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.io.IoVertexTest", method="shouldReadWriteDetachedVertexAsReferenceNoEdges", specific="graphson-v2-embedded", reason="Vertex.id() is deserialized as string, not class in graphson-v2-embedded."),
  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.io.IoVertexTest", method="shouldReadWriteVertexNoEdges", specific="graphson-v2-embedded", reason="Vertex.id() is deserialized as string, not class in graphson-v2-embedded."),
  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.io.IoVertexTest", method="shouldReadWriteVertexWithOUTEdges", specific="graphson-v2-embedded", reason="Vertex.id() is deserialized as string, not class in graphson-v2-embedded."),
  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.io.IoVertexTest", method="shouldReadWriteDetachedVertexNoEdges", specific="graphson-v2-embedded", reason="Vertex.id() is deserialized as string, not class in graphson-v2-embedded."),
  // passed: all, except graphson-v2-embedded.

  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.io.IoEdgeTest", method="shouldReadWriteDetachedEdgeAsReference", specific="graphson-v2-embedded", reason="no"),
  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.io.IoEdgeTest", method="shouldReadWriteEdge", specific="graphson-v2-embedded", reason="no"),
  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.io.IoEdgeTest", method="shouldReadWriteDetachedEdge", specific="graphson-v2-embedded", reason="no"),
  // passed: all, except graphson-v2-embedded.

  // TODO:
  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.io.IoGraphTest", method="*", reason="no"), // all failed.

  new Graph.OptOut(test="org.apache.tinkerpop.gremlin.structure.io.IoTest", method="*", reason="no")
  // all failed.
))
class S2Graph(_config: Config)(implicit val ec: ExecutionContext) extends S2GraphLike {

  import S2Graph._

  var apacheConfiguration: Configuration = _

  def dbSession() = scalikejdbc.AutoSession

  def this(apacheConfiguration: Configuration)(ec: ExecutionContext) = {
    this(S2Graph.toTypeSafeConfig(apacheConfiguration))(ec)
    this.apacheConfiguration = apacheConfiguration
  }

  private val running = new AtomicBoolean(true)

  val config = _config.withFallback(S2Graph.DefaultConfig)

  Model.apply(config)
  Model.loadCache()

  val MaxRetryNum = config.getInt("max.retry.number")
  val MaxBackOff = config.getInt("max.back.off")
  val BackoffTimeout = config.getInt("back.off.timeout")
  val DeleteAllFetchCount = config.getInt("delete.all.fetch.count")
  val DeleteAllFetchSize = config.getInt("delete.all.fetch.size")
  val FailProb = config.getDouble("hbase.fail.prob")
  val LockExpireDuration = config.getInt("lock.expire.time")
  val MaxSize = config.getInt("future.cache.max.size")
  val ExpireAfterWrite = config.getInt("future.cache.expire.after.write")
  val ExpireAfterAccess = config.getInt("future.cache.expire.after.access")
  val WaitTimeout = Duration(600, TimeUnit.SECONDS)

  val management = new Management(this)

  def getManagement() = management

  private val localLongId = new AtomicLong()

  def nextLocalLongId = localLongId.getAndIncrement()

  private def confWithFallback(conf: Config): Config = {
    conf.withFallback(config)
  }

  /**
    * TODO: we need to some way to handle malformed configuration for storage.
    */
  val storagePool: scala.collection.mutable.Map[String, Storage] = {
    val labels = Label.findAll()
    val services = Service.findAll()

    val labelConfigs = labels.flatMap(_.toStorageConfig)
    val serviceConfigs = services.flatMap(_.toStorageConfig)

    val configs = (labelConfigs ++ serviceConfigs).map { conf =>
      confWithFallback(conf)
    }.toSet

    val pools = new java.util.HashMap[Config, Storage]()
    configs.foreach { config =>
      pools.put(config, S2Graph.initStorage(this, config)(ec))
    }

    val m = new java.util.concurrent.ConcurrentHashMap[String, Storage]()

    labels.foreach { label =>
      if (label.storageConfigOpt.isDefined) {
        m += (s"label:${label.label}" -> pools(label.storageConfigOpt.get))
      }
    }

    services.foreach { service =>
      if (service.storageConfigOpt.isDefined) {
        m += (s"service:${service.serviceName}" -> pools(service.storageConfigOpt.get))
      }
    }

    m
  }

  val defaultStorage: Storage = S2Graph.initStorage(this, config)(ec)

  /** QueryLevel FutureCache */
  val queryFutureCache = new DeferCache[StepResult, Promise, Future](parseCacheConfig(config, "query."), empty = StepResult.Empty)

  for {
    entry <- config.entrySet() if S2Graph.DefaultConfigs.contains(entry.getKey)
    (k, v) = (entry.getKey, entry.getValue)
  } logger.info(s"[Initialized]: $k, ${this.config.getAnyRef(k)}")

  val indexProvider = IndexProvider.apply(config)

  val elementBuilder = new GraphElementBuilder(this)

  val traversalHelper = new TraversalHelper(this)

  def getStorage(service: Service): Storage = {
    storagePool.getOrElse(s"service:${service.serviceName}", defaultStorage)
  }

  def getStorage(label: Label): Storage = {
    storagePool.getOrElse(s"label:${label.label}", defaultStorage)
  }

  def flushStorage(): Unit = {
    storagePool.foreach { case (_, storage) =>

      /* flush is blocking */
      storage.flush()
    }
  }

  def fallback = Future.successful(StepResult.Empty)

  def checkEdges(edges: Seq[S2EdgeLike]): Future[StepResult] = {
    val futures = for {
      edge <- edges
    } yield {
      getStorage(edge.innerLabel).fetchSnapshotEdgeInner(edge).map { case (edgeOpt, _) =>
        edgeOpt.toSeq.map(e => EdgeWithScore(e, 1.0, edge.innerLabel))
      }
    }

    Future.sequence(futures).map { edgeWithScoreLs =>
      val edgeWithScores = edgeWithScoreLs.flatten
      StepResult(edgeWithScores = edgeWithScores, grouped = Nil, degreeEdges = Nil)
    }
  }

  //  def checkEdges(edges: Seq[Edge]): Future[StepResult] = storage.checkEdges(edges)

  def getEdges(q: Query): Future[StepResult] = {
    Try {
      if (q.steps.isEmpty) {
        // TODO: this should be get vertex query.
        fallback
      } else {
        val filterOutFuture = q.queryOption.filterOutQuery match {
          case None => fallback
          case Some(filterOutQuery) => getEdgesStepInner(filterOutQuery)
        }
        for {
          stepResult <- getEdgesStepInner(q)
          filterOutInnerResult <- filterOutFuture
        } yield {
          if (filterOutInnerResult.isEmpty) stepResult
          else StepResult.filterOut(this, q.queryOption, stepResult, filterOutInnerResult)
        }
      }
    } recover {
      case e: Exception =>
        logger.error(s"getEdgesAsync: $e", e)
        fallback
    } get
  }

  def getEdgesStepInner(q: Query, buildLastStepInnerResult: Boolean = false): Future[StepResult] = {
    Try {
      if (q.steps.isEmpty) fallback
      else {
        def fetch: Future[StepResult] = {
          val startStepInnerResult = QueryResult.fromVertices(this, q)
          q.steps.zipWithIndex.foldLeft(Future.successful(startStepInnerResult)) { case (prevStepInnerResultFuture, (step, stepIdx)) =>
            for {
              prevStepInnerResult <- prevStepInnerResultFuture
              currentStepInnerResult <- fetchStep(q, stepIdx, prevStepInnerResult, buildLastStepInnerResult)
            } yield {
              currentStepInnerResult.copy(
                accumulatedCursors = prevStepInnerResult.accumulatedCursors :+ currentStepInnerResult.cursors,
                failCount = currentStepInnerResult.failCount + prevStepInnerResult.failCount
              )
            }
          }
        }

        fetch
      }
    } recover {
      case e: Exception =>
        logger.error(s"getEdgesAsync: $e", e)
        fallback
    } get
  }

  def fetchStep(orgQuery: Query,
                stepIdx: Int,
                stepInnerResult: StepResult,
                buildLastStepInnerResult: Boolean = false): Future[StepResult] = {
    if (stepInnerResult.isEmpty) Future.successful(StepResult.Empty)
    else {
      val (_, prevStepTgtVertexIdEdges: Map[VertexId, ArrayBuffer[EdgeWithScore]], queryRequests: Seq[QueryRequest]) =
        traversalHelper.buildNextStepQueryRequests(orgQuery, stepIdx, stepInnerResult)

      val fetchedLs = fetches(queryRequests, prevStepTgtVertexIdEdges)

      traversalHelper.filterEdges(orgQuery, stepIdx, queryRequests,
        fetchedLs, orgQuery.steps(stepIdx).queryParams, buildLastStepInnerResult, prevStepTgtVertexIdEdges)(ec)
    }
  }


  /**
    * responsible to fire parallel fetch call into storage and create future that will return merged result.
    *
    * @param queryRequests
    * @param prevStepEdges
    * @return
    */
  def fetches(queryRequests: Seq[QueryRequest],
              prevStepEdges: Map[VertexId, Seq[EdgeWithScore]]): Future[Seq[StepResult]] = {

    val reqWithIdxs = queryRequests.zipWithIndex
    val requestsPerLabel = reqWithIdxs.groupBy(t => t._1.queryParam.label)
    val aggFuture = requestsPerLabel.foldLeft(Future.successful(Map.empty[Int, StepResult])) { case (prevFuture, (label, reqWithIdxs)) =>
      for {
        prev <- prevFuture
        cur <- getStorage(label).fetches(reqWithIdxs.map(_._1), prevStepEdges)
      } yield {
        prev ++ reqWithIdxs.map(_._2).zip(cur).toMap
      }
    }
    aggFuture.map { agg => agg.toSeq.sortBy(_._1).map(_._2) }
  }


  def getEdgesMultiQuery(mq: MultiQuery): Future[StepResult] = {
    Try {
      if (mq.queries.isEmpty) fallback
      else {
        val filterOutFuture = mq.queryOption.filterOutQuery match {
          case None => fallback
          case Some(filterOutQuery) => getEdgesStepInner(filterOutQuery)
        }

        val multiQueryFutures = Future.sequence(mq.queries.map { query => getEdges(query) })
        for {
          multiQueryResults <- multiQueryFutures
          filterOutInnerResult <- filterOutFuture
        } yield {
          StepResult.merges(mq.queryOption, multiQueryResults, mq.weights, filterOutInnerResult)
        }
      }
    } recover {
      case e: Exception =>
        logger.error(s"getEdgesAsync: $e", e)
        fallback
    } get
  }

  def getVertices(vertices: Seq[S2VertexLike]): Future[Seq[S2VertexLike]] = {
    val verticesWithIdx = vertices.zipWithIndex
    val futures = verticesWithIdx.groupBy { case (v, idx) => v.service }.map { case (service, vertexGroup) =>
      getStorage(service).fetchVertices(vertices).map(_.zip(vertexGroup.map(_._2)))
    }

    Future.sequence(futures).map { ls =>
      ls.flatten.toSeq.sortBy(_._2).map(_._1)
    }
  }

  def edgesAsync(vertex: S2VertexLike, direction: Direction, labelNames: String*): Future[util.Iterator[Edge]] = {
    val labelNameWithDirs =
      if (labelNames.isEmpty) {
        // TODO: Let's clarify direction
        if (direction == Direction.BOTH) {
          Label.findBySrcColumnId(vertex.id.colId).map(l => l.label -> Direction.OUT.name) ++
            Label.findByTgtColumnId(vertex.id.colId).map(l => l.label -> Direction.IN.name)
        } else if (direction == Direction.IN) {
          Label.findByTgtColumnId(vertex.id.colId).map(l => l.label -> direction.name)
        } else {
          Label.findBySrcColumnId(vertex.id.colId).map(l => l.label -> direction.name)
        }
      } else {
        direction match {
          case Direction.BOTH =>
            Seq(Direction.OUT, Direction.IN).flatMap { dir => labelNames.map(_ -> dir.name()) }
          case _ => labelNames.map(_ -> direction.name())
        }
      }

    fetchEdgesAsync(vertex, labelNameWithDirs.distinct)
  }

  /** mutate */
  def deleteAllAdjacentEdges(srcVertices: Seq[S2VertexLike],
                             labels: Seq[Label],
                             dir: Int,
                             ts: Long): Future[Boolean] = {

    val requestTs = ts
    val vertices = srcVertices
    /* create query per label */
    val queries = for {
      label <- labels
    } yield {
      val queryParam = QueryParam(labelName = label.label, direction = GraphUtil.fromDirection(dir),
        offset = 0, limit = DeleteAllFetchSize, duplicatePolicy = DuplicatePolicy.Raw)
      val step = Step(List(queryParam))
      Query(vertices, Vector(step))
    }

    val retryFuture = Extensions.retryOnSuccess(DeleteAllFetchCount) {
      fetchAndDeleteAll(queries, requestTs)
    } { case (allDeleted, deleteSuccess) =>
      allDeleted && deleteSuccess
    }.map { case (allDeleted, deleteSuccess) => allDeleted && deleteSuccess }

    retryFuture onFailure {
      case ex =>
        logger.error(s"[Error]: deleteAllAdjacentEdges failed.")
    }

    retryFuture
  }

  def fetchAndDeleteAll(queries: Seq[Query], requestTs: Long): Future[(Boolean, Boolean)] = {
    val futures = queries.map(getEdgesStepInner(_, true))
    val future = for {
      stepInnerResultLs <- Future.sequence(futures)
      (allDeleted, ret) <- deleteAllFetchedEdgesLs(stepInnerResultLs, requestTs)
    } yield {
      //        logger.debug(s"fetchAndDeleteAll: ${allDeleted}, ${ret}")
      (allDeleted, ret)
    }

    Extensions.retryOnFailure(MaxRetryNum) {
      future
    } {
      logger.error(s"fetch and deleteAll failed.")
      (true, false)
    }

  }

  def deleteAllFetchedEdgesLs(stepInnerResultLs: Seq[StepResult],
                              requestTs: Long): Future[(Boolean, Boolean)] = {
    stepInnerResultLs.foreach { stepInnerResult =>
      if (stepInnerResult.isFailure) throw new RuntimeException("fetched result is fallback.")
    }
    val futures = for {
      stepInnerResult <- stepInnerResultLs
      filtered = stepInnerResult.edgeWithScores.filter { edgeWithScore =>
        (edgeWithScore.edge.ts < requestTs) && !edgeWithScore.edge.isDegree
      }
      edgesToDelete = elementBuilder.buildEdgesToDelete(filtered, requestTs)
      if edgesToDelete.nonEmpty
    } yield {
      val head = edgesToDelete.head
      val label = head.edge.innerLabel
      val stepResult = StepResult(edgesToDelete, Nil, Nil, false)
      val ret = label.schemaVersion match {
        case HBaseType.VERSION3 | HBaseType.VERSION4 =>
          if (label.consistencyLevel == "strong") {
            /*
              * read: snapshotEdge on queryResult = O(N)
              * write: N x (relatedEdges x indices(indexedEdge) + 1(snapshotEdge))
              */
            mutateEdges(edgesToDelete.map(_.edge), withWait = true).map(_.forall(_.isSuccess))
          } else {
            getStorage(label).deleteAllFetchedEdgesAsyncOld(stepResult, requestTs, MaxRetryNum)
          }
        case _ =>

          /*
            * read: x
            * write: N x ((1(snapshotEdge) + 2(1 for incr, 1 for delete) x indices)
            */
          getStorage(label).deleteAllFetchedEdgesAsyncOld(stepResult, requestTs, MaxRetryNum)
      }
      ret
    }

    if (futures.isEmpty) {
      // all deleted.
      Future.successful(true -> true)
    } else {
      Future.sequence(futures).map { rets => false -> rets.forall(identity) }
    }
  }


  def mutateElements(elements: Seq[GraphElement],
                     withWait: Boolean = false): Future[Seq[MutateResponse]] = {

    val edgeBuffer = ArrayBuffer[(S2EdgeLike, Int)]()
    val vertexBuffer = ArrayBuffer[(S2VertexLike, Int)]()

    elements.zipWithIndex.foreach {
      case (e: S2EdgeLike, idx: Int) => edgeBuffer.append((e, idx))
      case (v: S2VertexLike, idx: Int) => vertexBuffer.append((v, idx))
      case any@_ => logger.error(s"Unknown type: ${any}")
    }

    val edgeFutureWithIdx = mutateEdges(edgeBuffer.map(_._1), withWait).map { result =>
      edgeBuffer.map(_._2).zip(result)
    }

    val vertexFutureWithIdx = mutateVertices(vertexBuffer.map(_._1), withWait).map { result =>
      vertexBuffer.map(_._2).zip(result)
    }

    val graphFuture = for {
      edgesMutated <- edgeFutureWithIdx
      verticesMutated <- vertexFutureWithIdx
    } yield (edgesMutated ++ verticesMutated).sortBy(_._1).map(_._2)

    graphFuture

  }

  def mutateEdges(edges: Seq[S2EdgeLike], withWait: Boolean = false): Future[Seq[MutateResponse]] = {
    val edgeWithIdxs = edges.zipWithIndex

    val (strongEdges, weakEdges) =
      edgeWithIdxs.partition { case (edge, idx) =>
        val e = edge
        e.innerLabel.consistencyLevel == "strong" && e.getOp() != GraphUtil.operations("insertBulk")
      }

    val weakEdgesFutures = weakEdges.groupBy { case (edge, idx) => edge.innerLabel.hbaseZkAddr }.map { case (zkQuorum, edgeWithIdxs) =>
      val futures = edgeWithIdxs.groupBy(_._1.innerLabel).map { case (label, edgeGroup) =>
        val storage = getStorage(label)
        val edges = edgeGroup.map(_._1)
        val idxs = edgeGroup.map(_._2)

        /* multiple edges with weak consistency level will be processed as batch */
        storage.mutateWeakEdges(zkQuorum, edges, withWait)
      }
      Future.sequence(futures)
    }
    val (strongDeleteAll, strongEdgesAll) = strongEdges.partition { case (edge, idx) => edge.getOp() == GraphUtil.operations("deleteAll") }

    val deleteAllFutures = strongDeleteAll.map { case (edge, idx) =>
      deleteAllAdjacentEdges(Seq(edge.srcVertex), Seq(edge.innerLabel), edge.getDir(), edge.ts).map(idx -> _)
    }

    val strongEdgesFutures = strongEdgesAll.groupBy { case (edge, idx) => edge.innerLabel }.map { case (label, edgeGroup) =>
      val edges = edgeGroup.map(_._1)
      val idxs = edgeGroup.map(_._2)
      val storage = getStorage(label)
      val zkQuorum = label.hbaseZkAddr
      storage.mutateStrongEdges(zkQuorum, edges, withWait = true).map { rets =>
        idxs.zip(rets)
      }
    }

    for {
      weak <- Future.sequence(weakEdgesFutures)
      deleteAll <- Future.sequence(deleteAllFutures)
      strong <- Future.sequence(strongEdgesFutures)
    } yield {
      (deleteAll ++ weak.flatten.flatten ++ strong.flatten).sortBy(_._1).map(r => new MutateResponse(r._2))
    }
  }

  def mutateVertices(vertices: Seq[S2VertexLike], withWait: Boolean = false): Future[Seq[MutateResponse]] = {
    def mutateVertices(storage: Storage)(zkQuorum: String, vertices: Seq[S2VertexLike],
                                         withWait: Boolean = false): Future[Seq[MutateResponse]] = {
      val futures = vertices.map { vertex => storage.mutateVertex(zkQuorum, vertex, withWait) }
      Future.sequence(futures)
    }

    val verticesWithIdx = vertices.zipWithIndex
    val futures = verticesWithIdx.groupBy { case (v, idx) => v.service }.map { case (service, vertexGroup) =>
      mutateVertices(getStorage(service))(service.cluster, vertexGroup.map(_._1), withWait).map(_.zip(vertexGroup.map(_._2)))
    }
    Future.sequence(futures).map { ls => ls.flatten.toSeq.sortBy(_._2).map(_._1) }
  }

  def incrementCounts(edges: Seq[S2EdgeLike], withWait: Boolean): Future[Seq[MutateResponse]] = {
    val edgesWithIdx = edges.zipWithIndex
    val futures = edgesWithIdx.groupBy { case (e, idx) => e.innerLabel }.map { case (label, edgeGroup) =>
      getStorage(label).incrementCounts(label.hbaseZkAddr, edgeGroup.map(_._1), withWait).map(_.zip(edgeGroup.map(_._2)))
    }
    Future.sequence(futures).map { ls =>
      ls.flatten.toSeq.sortBy(_._2).map(_._1)
    }
  }

  def updateDegree(edge: S2EdgeLike, degreeVal: Long = 0): Future[MutateResponse] = {
    val label = edge.innerLabel
    val storage = getStorage(label)

    storage.updateDegree(label.hbaseZkAddr, edge, degreeVal)
  }

  def isRunning(): Boolean = running.get()

  def shutdown(modelDataDelete: Boolean = false): Unit =
    if (running.compareAndSet(true, false)) {
      flushStorage()
      Model.shutdown(modelDataDelete)
      defaultStorage.shutdown()
      localLongId.set(0l)
    }


  def newEdge(srcVertex: S2VertexLike,
              tgtVertex: S2VertexLike,
              innerLabel: Label,
              dir: Int,
              op: Byte = GraphUtil.defaultOpByte,
              version: Long = System.currentTimeMillis(),
              propsWithTs: S2Edge.State,
              parentEdges: Seq[EdgeWithScore] = Nil,
              originalEdgeOpt: Option[S2EdgeLike] = None,
              pendingEdgeOpt: Option[S2EdgeLike] = None,
              statusCode: Byte = 0,
              lockTs: Option[Long] = None,
              tsInnerValOpt: Option[InnerValLike] = None): S2EdgeLike =
    elementBuilder.newEdge(srcVertex, tgtVertex, innerLabel, dir, op, version, propsWithTs,
      parentEdges, originalEdgeOpt, pendingEdgeOpt, statusCode, lockTs, tsInnerValOpt)

  def newVertexId(service: Service,
                  column: ServiceColumn,
                  id: Any): VertexId =
    elementBuilder.newVertexId(service, column, id)

  def newVertex(id: VertexId,
                ts: Long = System.currentTimeMillis(),
                props: S2Vertex.Props = S2Vertex.EmptyProps,
                op: Byte = 0,
                belongLabelIds: Seq[Int] = Seq.empty): S2VertexLike =
    elementBuilder.newVertex(id, ts, props, op, belongLabelIds)


  def getVertex(vertexId: VertexId): Option[S2VertexLike] = {
    val v = newVertex(vertexId)
    Await.result(getVertices(Seq(v)).map { vertices => vertices.headOption }, WaitTimeout)
  }

  def fetchEdges(vertex: S2VertexLike, labelNameWithDirs: Seq[(String, String)]): util.Iterator[Edge] = {
    Await.result(fetchEdgesAsync(vertex, labelNameWithDirs), WaitTimeout)
  }

  def fetchEdgesAsync(vertex: S2VertexLike, labelNameWithDirs: Seq[(String, String)]): Future[util.Iterator[Edge]] = {
    val queryParams = labelNameWithDirs.map { case (l, direction) =>
      QueryParam(labelName = l, direction = direction.toLowerCase)
    }

    val query = Query.toQuery(Seq(vertex), queryParams)
    val queryRequests = queryParams.map { param => QueryRequest(query, 0, vertex, param) }
    val ls = new util.ArrayList[Edge]()
    fetches(queryRequests, Map.empty).map { stepResultLs =>
      stepResultLs.foreach(_.edgeWithScores.foreach(es => ls.add(es.edge)))
      ls.iterator()
    }
  }

  def toVertex(serviceName: String,
               columnName: String,
               id: Any,
               props: Map[String, Any] = Map.empty,
               ts: Long = System.currentTimeMillis(),
               operation: String = "insert"): S2VertexLike =
    elementBuilder.toVertex(serviceName, columnName, id, props, ts, operation)

  def toEdge(srcId: Any,
             tgtId: Any,
             labelName: String,
             direction: String,
             props: Map[String, Any] = Map.empty,
             ts: Long = System.currentTimeMillis(),
             operation: String = "insert"): S2EdgeLike =
    elementBuilder.toEdge(srcId, tgtId, labelName, direction, props, ts, operation)

  def toGraphElement(s: String, labelMapping: Map[String, String] = Map.empty): Option[GraphElement] =
    elementBuilder.toGraphElement(s, labelMapping)

}
