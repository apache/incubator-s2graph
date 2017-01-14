package org.apache.s2graph.core.tinkerpop.structure

import org.apache.s2graph.core.S2Graph
import org.apache.s2graph.core.tinkerpop.S2GraphProvider
import org.apache.tinkerpop.gremlin.structure.StructureStandardSuite
import org.apache.tinkerpop.gremlin.{GraphProviderClass, LoadGraphWith}
import org.junit.FixMethodOrder
import org.junit.runner.RunWith
import org.junit.runners.MethodSorters

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@RunWith(classOf[StructureStandardSuite])
@GraphProviderClass(provider = classOf[S2GraphProvider], graph = classOf[S2Graph])
class S2GraphStructureStandardTest {

}