package org.apache.s2graph.core.io.tinkerpop.optimize;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.Ranging;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ElementValueComparator;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.javatuples.Pair;

import java.util.*;
import java.util.stream.Collectors;

public interface HasStepFolder<S, E> extends Step<S, E> {

    void addAll(Iterable<HasContainer> hasContainers);

    void orderBy(String key, Order order);

    void setLimit(int limit);

    int getLimit();

//    static boolean validJanusGraphHas(HasContainer has) {
//        if (has.getPredicate() instanceof AndP) {
//            final List<? extends P<?>> predicates = ((AndP<?>) has.getPredicate()).getPredicates();
//            return !predicates.stream().filter(p->!validJanusGraphHas(new HasContainer(has.getKey(), p))).findAny().isPresent();
//        } else {
//            return JanusGraphPredicate.Converter.supports(has.getBiPredicate());
//        }
//    }

//    static boolean validJanusGraphHas(Iterable<HasContainer> has) {
//        for (HasContainer h : has) {
//            if (!validJanusGraphHas(h)) return false;
//        }
//        return true;
//    }

//    static boolean validJanusGraphOrder(OrderGlobalStep ostep, Traversal rootTraversal,
//                                        boolean isVertexOrder) {
//        for (Pair<Traversal.Admin<Object, Comparable>, Comparator<Comparable>> comp : (List<Pair<Traversal.Admin<Object, Comparable>, Comparator<Comparable>>>) ostep.getComparators()) {
//            if (!(comp.getValue1() instanceof ElementValueComparator)) return false;
//            ElementValueComparator evc = (ElementValueComparator) comp.getValue1();
//            if (!(evc.getValueComparator() instanceof Order)) return false;
//
//            JanusGraphTransaction tx = JanusGraphTraversalUtil.getTx(rootTraversal.asAdmin());
//            String key = evc.getPropertyKey();
//            PropertyKey pkey = tx.getPropertyKey(key);
//            if (pkey == null || !(Comparable.class.isAssignableFrom(pkey.dataType()))) return false;
//            if (isVertexOrder && pkey.cardinality() != Cardinality.SINGLE) return false;
//        }
//        return true;
//    }

    static void foldInIds(final HasStepFolder s2graphStep, final Traversal.Admin<?, ?> traversal) {
        Step<?, ?> currentStep = s2graphStep.getNextStep();
        while (true) {
            if (currentStep instanceof HasContainerHolder) {
                Set<Object> ids = new HashSet<>();
                final GraphStep graphStep = (GraphStep) s2graphStep;
                for (final HasContainer hasContainer : ((HasContainerHolder) currentStep).getHasContainers()) {
                    if (GraphStep.processHasContainerIds(graphStep, hasContainer)) {
                        currentStep.getLabels().forEach(s2graphStep::addLabel);
                        if (!ids.isEmpty()) {
                            // intersect ids (shouldn't this be handled in TP GraphStep.processHasContainerIds?)
                            ids.stream().filter(id -> Arrays.stream(graphStep.getIds()).noneMatch(id::equals))
                                    .collect(Collectors.toSet()).forEach(ids::remove);
                            if (ids.isEmpty()) break;
                        } else {
                            Arrays.stream(graphStep.getIds()).forEach(ids::add);
                        }
                    }
                    // clear ids to allow folding in ids from next HasContainer if relevant
                    graphStep.clearIds();
                }
                graphStep.addIds(ids);
                if (!ids.isEmpty()) traversal.removeStep(currentStep);
            }
            else if (currentStep instanceof IdentityStep) {
                // do nothing, has no impact
            } else if (currentStep instanceof NoOpBarrierStep) {
                // do nothing, has no impact
            } else {
                break;
            }
            currentStep = currentStep.getNextStep();
        }
    }

    static void foldInHasContainer(final HasStepFolder s2graphStep, final Traversal.Admin<?, ?> traversal) {
        Step<?, ?> currentStep = s2graphStep.getNextStep();
        while (true) {
            if (currentStep instanceof HasContainerHolder) {
                Iterable<HasContainer> containers = ((HasContainerHolder) currentStep).getHasContainers();
//                if (validJanusGraphHas(containers)) {
                    s2graphStep.addAll(containers);
                    currentStep.getLabels().forEach(s2graphStep::addLabel);
                    traversal.removeStep(currentStep);
//                }
            } else if (currentStep instanceof IdentityStep) {
                // do nothing, has no impact
            } else if (currentStep instanceof NoOpBarrierStep) {
                // do nothing, has no impact
            } else {
                break;
            }
            currentStep = currentStep.getNextStep();
        }
    }

//    public static boolean addLabeledStepAsIdentity(Step<?,?> currentStep, final Traversal.Admin<?, ?> traversal) {
//        if (currentStep.getLabel().isPresent()) {
//            final IdentityStep identityStep = new IdentityStep<>(traversal);
//            identityStep.setLabel(currentStep.getLabel().get());
//            TraversalHelper.insertAfterStep(identityStep, currentStep, traversal);
//            return true;
//        } else return false;
//    }

    static void foldInOrder(final HasStepFolder s2graphStep, final Traversal.Admin<?, ?> traversal,
                            final Traversal<?, ?> rootTraversal, boolean isVertexOrder) {
        Step<?, ?> currentStep = s2graphStep.getNextStep();
        OrderGlobalStep<?, ?> lastOrder = null;
        while (true) {
            if (currentStep instanceof OrderGlobalStep) {
                if (lastOrder != null) { //Previous orders are rendered irrelevant by next order (since re-ordered)
                    lastOrder.getLabels().forEach(s2graphStep::addLabel);
                    traversal.removeStep(lastOrder);
                }
                lastOrder = (OrderGlobalStep) currentStep;
            } else if (currentStep instanceof IdentityStep) {
                // do nothing, can be skipped
            } else if (currentStep instanceof HasStep) {
                // do nothing, can be skipped
            } else if (currentStep instanceof NoOpBarrierStep) {
                // do nothing, can be skipped
            } else {
                break;
            }
            currentStep = currentStep.getNextStep();
        }

        if (lastOrder != null && lastOrder instanceof OrderGlobalStep) {
//            if (validJanusGraphOrder(lastOrder, rootTraversal, isVertexOrder)) {
                //Add orders to HasStepFolder
                for (Pair<Traversal.Admin<Object, Comparable>, Comparator<Comparable>> comp : (List<Pair<Traversal.Admin<Object, Comparable>, Comparator<Comparable>>>) ((OrderGlobalStep) lastOrder).getComparators()) {
                    ElementValueComparator evc = (ElementValueComparator) comp.getValue1();
                    s2graphStep.orderBy(evc.getPropertyKey(), (Order) evc.getValueComparator());
                }
                lastOrder.getLabels().forEach(s2graphStep::addLabel);
                traversal.removeStep(lastOrder);
//            }
        }
    }

//    static void splitAndP(final List<HasContainer> hasContainers, final Iterable<HasContainer> has) {
//        has.forEach(hasContainer -> {
//            if (hasContainer.getPredicate() instanceof AndP) {
//                for (final P<?> predicate : ((AndP<?>) hasContainer.getPredicate()).getPredicates()) {
//                    hasContainers.add(new HasContainer(hasContainer.getKey(), predicate));
//                }
//            } else
//                hasContainers.add(hasContainer);
//        });
//    }

    class OrderEntry {

        public final String key;
        public final Order order;

        public OrderEntry(String key, Order order) {
            this.key = key;
            this.order = order;
        }
    }

//    static <E extends Ranging> void foldInRange(final HasStepFolder s2graphStep, final Traversal.Admin<?, ?> traversal) {
//        Step<?, ?> nextStep = S2GraphTraversalUtil.getNextNonIdentityStep(s2graphStep);
//
//        if (nextStep instanceof RangeGlobalStep) {
//            RangeGlobalStep range = (RangeGlobalStep) nextStep;
//            int limit = 10;
//            s2graphStep.setLimit(limit);
//            if (range.getLowRange() == 0) { //Range can be removed since there is no offset
//                nextStep.getLabels().forEach(s2graphStep::addLabel);
//                traversal.removeStep(nextStep);
//            }
//        }
//    }


}
