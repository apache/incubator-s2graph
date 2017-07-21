package org.apache.s2graph.core.io.tinkerpop.optimize;

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



import org.apache.s2graph.core.Query;
import org.apache.s2graph.core.S2Vertex;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Profiling;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

//public class S2GraphVertexStep<E extends Element> extends VertexStep<E> implements HasStepFolder<Vertex, E>, Profiling {
//
//    public S2GraphVertexStep(VertexStep<E> originalStep) {
//        super(originalStep.getTraversal(), originalStep.getReturnClass(), originalStep.getDirection(), originalStep.getEdgeLabels());
//        originalStep.getLabels().forEach(this::addLabel);
//        this.hasContainers = new ArrayList<>();
//        this.limit = Integer.MAX_VALUE;
//    }
//
//    private boolean initialized = false;
//    private Map<S2Vertex, Iterable<? extends Element>> multiQueryResults = null;
//
//
//    public Query makeQuery(Query query) {
//        query.labels(getEdgeLabels());
//        query.direction(getDirection());
//        for (HasContainer condition : hasContainers) {
//            query.has(condition.getKey(), JanusGraphPredicate.Converter.convert(condition.getBiPredicate()), condition.getValue());
//        }
//        for (OrderEntry order : orders) query.orderBy(order.key, order.order);
//        if (limit != BaseQuery.NO_LIMIT) query.limit(limit);
//        ((BasicVertexCentricQueryBuilder) query).profiler(queryProfiler);
//        return query;
//    }
//
//    @SuppressWarnings("deprecation")
//    private void initialize() {
//        assert !initialized;
//        initialized = true;
//
//        if (!starts.hasNext()) throw FastNoSuchElementException.instance();
//        JanusGraphMultiVertexQuery mquery = JanusGraphTraversalUtil.getTx(traversal).multiQuery();
//        List<Traverser.Admin<Vertex>> vertices = new ArrayList<>();
//        starts.forEachRemaining(v -> {
//            vertices.add(v);
//            mquery.addVertex(v.get());
//        });
//        starts.add(vertices.iterator());
//        assert vertices.size() > 0;
//        makeQuery(mquery);
//
//        multiQueryResults = (Vertex.class.isAssignableFrom(getReturnClass())) ? mquery.vertices() : mquery.edges();
//
//    }
//
//    @Override
//    protected Traverser.Admin<E> processNextStart() {
//        if (!initialized) initialize();
//        return super.processNextStart();
//    }
//
//    @Override
//    protected Iterator<E> flatMap(final Traverser.Admin<Vertex> traverser) {
//        JanusGraphVertexQuery query = makeQuery((JanusGraphTraversalUtil.getJanusGraphVertex(traverser)).query());
//        return (Vertex.class.isAssignableFrom(getReturnClass())) ? query.vertices().iterator() : query.edges().iterator();
//    }
//
//    @Override
//    public void reset() {
//        super.reset();
//        this.initialized = false;
//    }
//
//    @Override
//    public S2GraphVertexStep<E> clone() {
//        final S2GraphVertexStep<E> clone = (S2GraphVertexStep<E>) super.clone();
//        clone.initialized = false;
//        return clone;
//    }
//
//
//    private final List<HasContainer> hasContainers;
//    private int limit = Integer.MAX_VALUE;
//    private List<OrderEntry> orders = new ArrayList<>();
//
//
//    @Override
//    public void addAll(Iterable<HasContainer> has) {
//        HasStepFolder.splitAndP(hasContainers, has);
//    }
//
//    @Override
//    public void orderBy(String key, Order order) {
//        orders.add(new OrderEntry(key, order));
//    }
//
//    @Override
//    public void setLimit(int limit) {
//        this.limit = limit;
//    }
//
//    @Override
//    public int getLimit() {
//        return this.limit;
//    }
//
//    @Override
//    public String toString() {
//        return this.hasContainers.isEmpty() ? super.toString() : StringFactory.stepString(this, this.hasContainers);
//    }
//
//    @Override
//    public void setMetrics(MutableMetrics metrics) {
////        queryProfiler = new TP3ProfileWrapper(metrics);
//    }
//}

