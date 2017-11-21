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

import com.typesafe.config.Config
import org.apache.tinkerpop.gremlin.structure.Graph

import scala.concurrent.ExecutionContext


@Graph.OptIns(value = Array(
  new Graph.OptIn(value = Graph.OptIn.SUITE_PROCESS_STANDARD),
  new Graph.OptIn(value = Graph.OptIn.SUITE_STRUCTURE_STANDARD)
))
@Graph.OptOuts(value = Array(
  /* Process */
  /* branch: passed all. */
  /* filter */
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.DropTest$Traversals", method = "g_V_properties_drop", reason = "please find bug on this case."),

  /* map */
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.CountTest$Traversals", method = "g_V_both_both_count", reason = "count differ very little. fix this."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.CountTest$Traversals", method = "g_V_repeatXoutX_timesX3X_count", reason = "count differ very little. fix this."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.CountTest$Traversals", method = "g_V_repeatXoutX_timesX8X_count", reason = "count differ very litter. fix this."),

  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest$Traversals", method = "testProfileStrategyCallback", reason = "NullPointerException. fix this."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest$Traversals", method = "g_V_whereXinXcreatedX_count_isX1XX_name_profile", reason = "java.lang.AssertionError: There should be 3 top-level metrics. fix this."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest$Traversals", method = "g_V_whereXinXcreatedX_count_isX1XX_name_profileXmetricsX", reason = "expected 2, actual 6. fix this."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest$Traversals", method = "grateful_V_out_out_profileXmetricsX", reason = "expected 8049, actual 8046. fix this."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest$Traversals", method = "grateful_V_out_out_profile", reason = "expected 8049, actual 8046. fix this."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest$Traversals", method = "modern_V_out_out_profileXmetricsX", reason = "expected 2, actual 6. fix this."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest$Traversals", method = "modern_V_out_out_profile", reason = "expected 2, actual 6. fix this."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest$Traversals", method = "testProfileStrategyCallbackSideEffect", reason = "NullPointerException. fix this."),

  /* sideEffect */
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SubgraphTest$Traversals", method = "g_V_withSideEffectXsgX_repeatXbothEXcreatedX_subgraphXsgX_outVX_timesX5X_name_dedup", reason = "Expected 5, Actual 6."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SubgraphTest$Traversals", method = "g_V_withSideEffectXsgX_outEXknowsX_subgraphXsgX_name_capXsgX", reason = "Expected 3, Actual 6"),

  /* compliance */
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.CoreTraversalTest", method = "shouldThrowExceptionWhenIdsMixed", reason = "VertexId is not Element."),

  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.TraversalInterruptionTest", method = "*", reason = "not supported yet."),

  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ElementIdStrategyProcessTest", method = "shouldGenerateDefaultIdOnAddVWithSpecifiedId", reason = "GraphStep.processNextStart throw FastNoSuchElementException when GraphStep.start = true and GraphStep.end = true."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ElementIdStrategyProcessTest", method = "shouldGenerateDefaultIdOnAddVWithGeneratedCustomId", reason = "GraphStep.processNextStart throw FastNoSuchElementException when GraphStep.start = true and GraphStep.end = true."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ElementIdStrategyProcessTest", method = "shouldGenerateDefaultIdOnGraphAddVWithGeneratedDefaultId", reason = "GraphStep.processNextStart throw FastNoSuchElementException when GraphStep.start = true and GraphStep.end = true."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ElementIdStrategyProcessTest", method = "shouldGenerateDefaultIdOnAddVWithGeneratedDefaultId", reason = "GraphStep.processNextStart throw FastNoSuchElementException when GraphStep.start = true and GraphStep.end = true."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ElementIdStrategyProcessTest", method = "shouldGenerateDefaultIdOnGraphAddVWithGeneratedCustomId", reason = "GraphStep.processNextStart throw FastNoSuchElementException when GraphStep.start = true and GraphStep.end = true."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ElementIdStrategyProcessTest", method = "shouldGenerateDefaultIdOnGraphAddVWithSpecifiedId", reason = "GraphStep.processNextStart throw FastNoSuchElementException when GraphStep.start = true and GraphStep.end = true."),

  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategyProcessTest", method = "*", reason = "not supported yet."),

  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.PartitionStrategyProcessTest", method = "*", reason = "not supported yet."),

  /* Structure */
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.EdgeTest$BasicEdgeTest", method = "shouldValidateIdEquality", reason = "reference equals on EdgeId is not supported."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.EdgeTest$BasicEdgeTest", method = "shouldValidateEquality", reason = "reference equals on EdgeId is not supported."),

  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.VertexTest$BasicVertexTest", method = "shouldHaveExceptionConsistencyWhenAssigningSameIdOnEdge", reason = "S2Vertex.addEdge behave as upsert."),

  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdgeTest", method = "shouldNotEvaluateToEqualDifferentId", reason = "reference equals is not supported."),

  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.util.detached.DetachedPropertyTest", method = "shouldNotBeEqualPropertiesAsThereIsDifferentKey", reason = "reference equals is not supported."),

  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.GraphTest", method = "shouldRemoveVertices", reason = "random label creation is not supported. all label need to be pre-configured."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.GraphTest", method = "shouldHaveExceptionConsistencyWhenAssigningSameIdOnVertex", reason = "Assigning the same ID to an Element update instead of throwing exception."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.GraphTest", method = "shouldRemoveEdges", reason = "random label creation is not supported. all label need to be pre-configured."),

  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceEdgeTest", method = "shouldNotEvaluateToEqualDifferentId", reason = "Assigning the same ID to an Element update instead of throwing exception."),

  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.algorithm.generator.CommunityGeneratorTest$DifferentDistributionsTest", method = "shouldGenerateDifferentGraph", specific = "test(NormalDistribution{stdDeviation=2.0, mean=0.0},PowerLawDistribution{gamma=2.4, multiplier=0.0},0.1)", reason = "graphson-v2-embedded is not supported."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.algorithm.generator.CommunityGeneratorTest$DifferentDistributionsTest", method = "shouldGenerateDifferentGraph", specific = "test(NormalDistribution{stdDeviation=2.0, mean=0.0},PowerLawDistribution{gamma=2.4, multiplier=0.0},0.5)", reason = "graphson-v2-embedded is not supported."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.algorithm.generator.CommunityGeneratorTest$DifferentDistributionsTest", method = "shouldGenerateDifferentGraph", specific = "test(NormalDistribution{stdDeviation=2.0, mean=0.0},NormalDistribution{stdDeviation=4.0, mean=0.0},0.5)", reason = "graphson-v2-embedded is not supported."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.algorithm.generator.CommunityGeneratorTest$DifferentDistributionsTest", method = "shouldGenerateDifferentGraph", specific = "test(NormalDistribution{stdDeviation=2.0, mean=0.0},NormalDistribution{stdDeviation=4.0, mean=0.0},0.1)", reason = "graphson-v2-embedded is not supported."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.algorithm.generator.CommunityGeneratorTest$DifferentDistributionsTest", method = "shouldGenerateDifferentGraph", specific = "test(PowerLawDistribution{gamma=2.3, multiplier=0.0},PowerLawDistribution{gamma=2.4, multiplier=0.0},0.25)", reason = "graphson-v2-embedded is not supported."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.algorithm.generator.CommunityGeneratorTest$DifferentDistributionsTest", method = "shouldGenerateDifferentGraph", specific = "test(PowerLawDistribution{gamma=2.3, multiplier=0.0},NormalDistribution{stdDeviation=4.0, mean=0.0},0.25)", reason = "graphson-v2-embedded is not supported."),

  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.algorithm.generator.DistributionGeneratorTest", method = "*", reason = "non-deterministic test."),

  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.SerializationTest$GryoTest", method = "shouldSerializeTree", reason = "order of children is reversed. not sure why."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.SerializationTest$GraphSONTest", method = "shouldSerializeTraversalMetrics", reason = "expected 2, actual 3."),

  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.io.IoVertexTest", method = "shouldReadWriteVertexWithBOTHEdges", specific = "graphson-v2-embedded", reason = "Vertex.id() is deserialized as string, not class in graphson-v2-embedded."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.io.IoVertexTest", method = "shouldReadWriteVertexWithINEdges", specific = "graphson-v2-embedded", reason = "Vertex.id() is deserialized as string, not class in graphson-v2-embedded."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.io.IoVertexTest", method = "shouldReadWriteDetachedVertexAsReferenceNoEdges", specific = "graphson-v2-embedded", reason = "Vertex.id() is deserialized as string, not class in graphson-v2-embedded."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.io.IoVertexTest", method = "shouldReadWriteVertexNoEdges", specific = "graphson-v2-embedded", reason = "Vertex.id() is deserialized as string, not class in graphson-v2-embedded."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.io.IoVertexTest", method = "shouldReadWriteVertexWithOUTEdges", specific = "graphson-v2-embedded", reason = "Vertex.id() is deserialized as string, not class in graphson-v2-embedded."),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.io.IoVertexTest", method = "shouldReadWriteDetachedVertexNoEdges", specific = "graphson-v2-embedded", reason = "Vertex.id() is deserialized as string, not class in graphson-v2-embedded."),

  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.io.IoEdgeTest", method = "shouldReadWriteDetachedEdgeAsReference", specific = "graphson-v2-embedded", reason = "no"),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.io.IoEdgeTest", method = "shouldReadWriteEdge", specific = "graphson-v2-embedded", reason = "no"),
  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.io.IoEdgeTest", method = "shouldReadWriteDetachedEdge", specific = "graphson-v2-embedded", reason = "no"),

  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.io.IoGraphTest", method = "*", reason = "no"), // all failed.

  new Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.io.IoTest", method = "*", reason = "no")
))
class S2GraphTp(config: Config)(override implicit val ec: ExecutionContext) extends S2Graph(config) {

}
