//package org.apache.s2graph.core.tinkerpop.optimize
//
//import org.apache.s2graph.core.utils.logger
//import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep
//import org.apache.tinkerpop.gremlin.process.traversal.{Step, Traversal, TraversalStrategy}
//import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy
//import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper
//import org.apache.tinkerpop.gremlin.structure.Element
//
//object S2GraphStepStrategy {
//  val INSTANCE = new S2GraphStepStrategy()
//  def instance = INSTANCE
//}
//class S2GraphStepStrategy extends AbstractTraversalStrategy[TraversalStrategy.ProviderOptimizationStrategy] with TraversalStrategy.ProviderOptimizationStrategy {
//  import scala.collection.JavaConversions._
//  override def apply(traversal: Traversal.Admin[_, _]): Unit = {
//    TraversalHelper.getStepsOfClass(classOf[GraphStep[_, Element]], traversal).foreach { originalGraphStep =>
//      if (originalGraphStep.getIds() == null || originalGraphStep.getIds().length == 0) {
//        //Try to optimize for index calls
//
//        val s2GraphStep = new S2GraphStep(originalGraphStep)
//        TraversalHelper.replaceStep(originalGraphStep, s2GraphStep.asInstanceOf[Step[_ <: Any, _ <: Any]], traversal)
//        logger.error(s"[[Ids is empty]]")
//      } else {
////        //Make sure that any provided "start" elements are instantiated in the current transaction
////        Object[] ids = originalGraphStep.getIds();
////        ElementUtils.verifyArgsMustBeEitherIdorElement(ids);
////        if (ids[0] instanceof Element) {
////          //GraphStep constructor ensures that the entire array is elements
////          final Object[] elementIds = new Object[ids.length];
////          for (int i = 0; i < ids.length; i++) {
////            elementIds[i] = ((Element) ids[i]).id();
////          }
////          originalGraphStep.setIteratorSupplier(() -> (Iterator) (originalGraphStep.returnsVertex() ?
////            ((Graph) originalGraphStep.getTraversal().getGraph().get()).vertices(elementIds) :
////            ((Graph) originalGraphStep.getTraversal().getGraph().get()).edges(elementIds)));
////        }
//        logger.error(s"[[Ids is not empty]]")
//      }
//    }
//
//  }
//}
