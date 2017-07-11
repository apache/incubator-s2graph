package org.apache.s2graph.core.io.tinkerpop.optimize;

import org.apache.s2graph.core.utils.logger;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.Profiling;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.*;

public class S2GraphStep<S, E extends Element> extends GraphStep<S, E> {
    private final List<HasContainer> hasContainers = new ArrayList<>();


    public S2GraphStep(final GraphStep<S, E> originalStep) {
        super(originalStep.getTraversal(), originalStep.getReturnClass(), originalStep.isStartStep(), originalStep.getIds());
        originalStep.getLabels().forEach(this::addLabel);
        System.err.println("[[S2GraphStep]]");
    }

    @Override
    public String toString() {
        return this.hasContainers.isEmpty() ?
                super.toString() : StringFactory.stepString(this, Arrays.toString(this.ids), this.hasContainers);
    }

}
