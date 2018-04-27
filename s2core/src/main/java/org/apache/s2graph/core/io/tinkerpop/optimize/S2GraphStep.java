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


import org.apache.s2graph.core.EdgeId;
import org.apache.s2graph.core.S2Graph;
import org.apache.s2graph.core.types.VertexId;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import java.util.*;
import java.util.stream.Collectors;

public class S2GraphStep<S, E extends Element> extends GraphStep<S, E> {
    private final List<HasContainer> hasContainers = new ArrayList<>();

    private void foldInHasContainers(final GraphStep<?, ?> originalStep) {
        Step<?, ?> currentStep = originalStep.getNextStep();
        while (true) {
            if (currentStep instanceof HasStep) {
                hasContainers.addAll(((HasStep)currentStep).getHasContainers());
            } else if (currentStep instanceof IdentityStep) {

            } else if (currentStep instanceof NoOpBarrierStep) {

            } else {
                break;
            }

            currentStep = currentStep.getNextStep();
        }

    }
    public S2GraphStep(final GraphStep<S, E> originalStep) {
        super(originalStep.getTraversal(), originalStep.getReturnClass(), originalStep.isStartStep(), originalStep.getIds());

        if (!(traversal.asAdmin().getGraph().get() instanceof S2Graph)) return ;

        foldInHasContainers(originalStep);
        originalStep.getLabels().forEach(this::addLabel);
        // 1. build S2Graph QueryParams for this step.
        // 2. graph.vertices(this.ids, queryParams) or graph.edges(this.ids, queryParams)
        // 3. vertices/edges lookup indexProvider, then return Seq[EdgeId/VertexId]

        this.setIteratorSupplier(() -> {
            final S2Graph graph = (S2Graph)traversal.asAdmin().getGraph().get();
            if (this.ids != null && this.ids.length > 0) {

                return iteratorList((Iterator)graph.vertices(this.ids));
            }
            // full scan
            Boolean isVertex = Vertex.class.isAssignableFrom(this.returnClass);
            if (hasContainers.isEmpty()) {
                return (Iterator) (isVertex ? graph.vertices() : graph.edges());
            } else {
                List<VertexId> vids = new ArrayList<>();
                List<EdgeId> eids = new ArrayList<>();
                List<HasContainer> filtered = new ArrayList<>();

                hasContainers.forEach(c -> {
                    if (c.getKey() == T.id.getAccessor()) {
                        if (c.getValue() instanceof List<?>) {
                            ((List<?>)c.getValue()).forEach(o -> {
                                if (isVertex) vids.add((VertexId) o);
                                else eids.add((EdgeId) o);
                            });
                        } else {
                            if (isVertex) vids.add((VertexId)c.getValue());
                            else eids.add((EdgeId)c.getValue());
                        }
                    } else {
                        filtered.add(c);
                    }
                });

                if (isVertex) {
                    List<VertexId> ids = graph.indexProvider().fetchVertexIds(filtered.stream().distinct().collect(Collectors.toList()));
                    if (ids.isEmpty()) return (Iterator) graph.vertices();
                    else return (Iterator) graph.vertices(ids.toArray());
                } else {
                    List<EdgeId> ids = graph.indexProvider().fetchEdgeIds(filtered.stream().distinct().collect(Collectors.toList()));
                    if (ids.isEmpty()) return (Iterator) graph.edges();
                    else return (Iterator) graph.edges(ids.toArray());
                }
            }
        });
    }

    @Override
    public String toString() {
        return this.hasContainers.isEmpty() ?
                super.toString() : StringFactory.stepString(this, Arrays.toString(this.ids), this.hasContainers);
    }

    private <E extends Element> Iterator<E> iteratorList(final Iterator<E> iterator) {
        final List<E> list = new ArrayList<E>();
        while (iterator.hasNext()) {
            final E e = iterator.next();
            if (HasContainer.testAll(e, this.hasContainers))
                list.add(e);
        }
        return list.iterator();
    }

}
