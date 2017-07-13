package org.apache.s2graph.core.io.tinkerpop.optimize;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.Iterator;


public class S2GraphStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {

    private static final S2GraphStepStrategy INSTANCE = new S2GraphStepStrategy();

    private S2GraphStepStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (TraversalHelper.onGraphComputer(traversal))
            return;

        TraversalHelper.getStepsOfClass(GraphStep.class, traversal).forEach(originalGraphStep -> {
            if (originalGraphStep.getIds() == null || originalGraphStep.getIds().length == 0) {
                //Try to optimize for index calls
                final S2GraphStep<?, ?> s2GraphStep = new S2GraphStep<>(originalGraphStep);
                TraversalHelper.replaceStep(originalGraphStep, (Step) s2GraphStep, traversal);

            } else {
                Object[] ids = originalGraphStep.getIds();
                if (ids[0] instanceof Element) {
                    //GraphStep constructor ensures that the entire array is elements
                    final Object[] elementIds = new Object[ids.length];
                    for (int i = 0; i < ids.length; i++) {
                        elementIds[i] = ((Element) ids[i]).id();
                    }
                    originalGraphStep.setIteratorSupplier(() -> (Iterator) (originalGraphStep.returnsVertex() ?
                            ((Graph) originalGraphStep.getTraversal().getGraph().get()).vertices(elementIds) :
                            ((Graph) originalGraphStep.getTraversal().getGraph().get()).edges(elementIds)));
                }
            }
        });
    }

    public static S2GraphStepStrategy instance() {
        return INSTANCE;
    }
}
