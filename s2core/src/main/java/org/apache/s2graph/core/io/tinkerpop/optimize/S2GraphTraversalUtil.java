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

//package org.apache.s2graph.core.io.tinkerpop.optimize;
//
//import org.apache.s2graph.core.S2Vertex;
//import org.apache.tinkerpop.gremlin.process.traversal.Step;
//import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
//import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
//import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
//import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
//import org.apache.tinkerpop.gremlin.structure.Edge;
//import org.apache.tinkerpop.gremlin.structure.Element;
//import org.apache.tinkerpop.gremlin.structure.Graph;
//import org.apache.tinkerpop.gremlin.structure.Vertex;
//import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedVertex;
//
//import java.util.Optional;
//
//public class S2GraphTraversalUtil {
//
//    public static S2Vertex getS2Vertex(Element v) {
//        while (v instanceof WrappedVertex) {
//            v = ((WrappedVertex<Vertex>) v).getBaseVertex();
//        }
//        if (v instanceof S2Vertex) {
//            return (S2Vertex) v;
//        } else throw new IllegalArgumentException("Expected traverser of JanusGraph vertex but found: " + v);
//    }
//
//    public static S2Vertex getS2Vertex(Traverser<? extends Element> traverser) {
//        return getS2Vertex(traverser.get());
//    }
//
//
//    public static Step getNextNonIdentityStep(final Step start) {
//        Step currentStep = start.getNextStep();
//        //Skip over identity steps
//        while (currentStep instanceof IdentityStep) currentStep = currentStep.getNextStep();
//        return currentStep;
//    }
//
//
//}
