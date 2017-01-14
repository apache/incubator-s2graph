package org.apache.s2graph.core.tinkerpop.structure

import org.apache.s2graph.core.S2Graph
import org.apache.s2graph.core.tinkerpop.S2GraphProvider
import org.apache.tinkerpop.gremlin.GraphProviderClass
import org.apache.tinkerpop.gremlin.structure.{StructureIntegrateSuite, StructureStandardSuite}
import org.junit.runner.RunWith

@RunWith(classOf[StructureIntegrateSuite])
@GraphProviderClass(provider = classOf[S2GraphProvider], graph = classOf[S2Graph])
class S2GraphStructureIntegrateTest {

}
