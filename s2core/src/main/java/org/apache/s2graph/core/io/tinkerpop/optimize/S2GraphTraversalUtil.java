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
