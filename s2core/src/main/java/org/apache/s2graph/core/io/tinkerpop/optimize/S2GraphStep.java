package org.apache.s2graph.core.io.tinkerpop.optimize;

import org.apache.s2graph.core.EdgeId;
import org.apache.s2graph.core.QueryParam;
import org.apache.s2graph.core.S2Graph;
import org.apache.s2graph.core.index.IndexProvider;
import org.apache.s2graph.core.index.IndexProvider$;
import org.apache.s2graph.core.mysqls.Label;
import org.apache.s2graph.core.utils.logger;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.Profiling;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

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

            String queryString = IndexProvider$.MODULE$.buildQueryString(hasContainers);

            List<String> ids = graph.indexProvider().fetchIds(queryString);
            return (Iterator) (Vertex.class.isAssignableFrom(this.returnClass) ? graph.vertices(ids) : graph.edges(ids));
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
