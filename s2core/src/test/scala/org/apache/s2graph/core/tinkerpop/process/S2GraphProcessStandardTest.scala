package org.apache.s2graph.core.tinkerpop.process

import org.apache.s2graph.core.S2Graph
import org.apache.s2graph.core.tinkerpop.S2GraphProvider
import org.apache.tinkerpop.gremlin.GraphProviderClass
import org.apache.tinkerpop.gremlin.process.ProcessStandardSuite
import org.junit.FixMethodOrder
import org.junit.runner.RunWith
import org.junit.runners.MethodSorters

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@RunWith(classOf[ProcessStandardSuite])
@GraphProviderClass(provider = classOf[S2GraphProvider], graph = classOf[S2Graph])
class S2GraphProcessStandardTest {

}