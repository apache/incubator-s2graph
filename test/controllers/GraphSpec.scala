package controllers

import com.daumkakao.s2graph.core.Management._
import com.daumkakao.s2graph.core._
import org.specs2.mutable.Specification
import play.api.libs.json.Json
import scala.Option.option2Iterable
/**
 * test-only kgraph.GraphSpec
 */
class GraphSpec extends Specification {
  sequential

  private val phase = "alpha"
  //  Graph.tableName = tableName
  System.setProperty("phase", phase)
//  Config.phase = phase
  private val serviceName = "s2graph"
  private val labelName = "graph_test"
    
  val msgs = """
    ["1417079166891\tu\te\t156598730\t158524147\tgraph_test\t{\"is_blocked\":true}","1417079166000\td\te\t4079848\t54739565\tgraph_test","1417079166000\ti\te\t102735518\t49629673\tgraph_test","1417079166000\ti\te\t15120422\t160902634\tgraph_test","1417079166000\ti\te\t160251573\t7960026\tgraph_test","1417079167000\td\te\t816808\t47823955\tgraph_test","1417079166000\ti\te\t160251573\t43063061\tgraph_test","1417079167000\ti\te\t141391508\t151985238\tgraph_test","1417079167360\tu\te\t43237903\t49005293\tgraph_test\t{\"is_blocked\":true}","1417079167000\ti\te\t9788808\t167206573\tgraph_test","1417079167000\ti\te\t106040854\t166946583\tgraph_test","1417079166000\ti\te\t15120422\t35674614\tgraph_test","1417079167519\tu\te\t29460631\t28640554\tgraph_test\t{\"is_blocked\":false}","1417079166000\ti\te\t15120422\t6366975\tgraph_test","1417079167000\tu\te\t4610363\t146176454\tgraph_test\t{\"is_hidden\":true}","1417079167000\td\te\t4935659\t1430321\tgraph_test","1417079166000\ti\te\t15120422\t136919643\tgraph_test","1417079167000\td\te\t84547561\t17923231\tgraph_test","1417079167000\td\te\t108876864\t19695266\tgraph_test","1417079166000\td\te\t14644128\t11853672\tgraph_test","1417079167000\ti\te\t142522237\t167208738\tgraph_test"]"""

  val createService = """{"serviceName": "s2graph"}"""

  val createLabel = """
  {"label": "graph_test", "srcServiceName": "s2graph", "srcColumnName": "account_id", "srcColumnType": "long", "tgtServiceName": "s2graph", "tgtColumnName": "account_id", "tgtColumnType": "long", "indexProps": {"time": 0, "weight":0}}
  """
    
  // 0.
  AdminController.createServiceInner(Json.parse(createService))
  // 1.   
  AdminController.deleteLabel(labelName)
  // 2. 
  AdminController.createLabelInner(Json.parse(createLabel))
  
  val label = Label.findByName("graph_test").get
  
  val insertEdges = """
[
  {"from":1,"to":101,"label":"graph_test","props":{"time":-1, "weight":10},"timestamp":193829192},
  {"from":1,"to":102,"label":"graph_test","props":{"time":0, "weight":11},"timestamp":193829193},
  {"from":1,"to":103,"label":"graph_test","props":{"time":1, "weight":12},"timestamp":193829194},
  {"from":1,"to":104,"label":"graph_test","props":{"time":-2, "weight":1},"timestamp":193829195}
]
  """
//  val insertResults = List(
//  Edge(Vertex(CompositeId(label.srcColumn.id.get, InnerVal.withLong(1), true)), Vertex(CompositeId(label.tgtColumn.id.get, InnerVal.withLong(103), true)), LabelWithDirection(label.id.get, 0), 0, 193829192, )
//  )
  val insertEdgesTsv = """["193829192\ti\te\t1\t101\tgraph_test\t{\"time\":1, \"weight\": 10, \"is_hidden\": false}","193829192\ti\te\t1\t102\tgraph_test\t{\"time\":1, \"weight\": 19, \"is_hidden\": false}","193829192\ti\te\t1\t103\tgraph_test\t{\"time\":1, \"weight\": 10, \"is_hidden\": false}"]"""

  val updateEdges = """
[
  {"from":1,"to":102,"label":"graph_test","timestamp":193829199, "props": {"is_hidden":true}},
  {"from":1,"to":102,"label":"graph_test","timestamp":193829299, "props": {"weight":100}}
]
  """
  val incrementEdges = """
[
  {"from":1,"to":102,"label":"graph_test","timestamp":193829399, "props": {"weight":100}},
  {"from":1,"to":102,"label":"graph_test","timestamp":193829499, "props": {"weight":100}},
  {"from":1,"to":103,"label":"graph_test","timestamp":193829599, "props": {"weight":1000}},
  {"from":1,"to":102,"label":"graph_test","timestamp":193829699, "props": {"weight":-200}},
  {"from":1,"to":102,"label":"graph_test","timestamp":193829699, "props": {"weight":400}} 
]   
  """

  val queryEdges = """
{
    "srcVertices": [{"columnName": "account_id", "id":1}],
    "steps": [
      [{"label": "graph_test", "direction": "out", "limit": 100, "scoring":{"time": 0, "weight": 1}}]
    ]
}
  """
  val query = toQuery(Json.parse(queryEdges))

  "RequestParsor" should {
    "Json parsed edge should match Graph.toEdge" in {
      val jsons = Json.parse(insertEdges)
      val jsonEdges = toEdges(jsons, "insert")
      val tsvs = GraphUtil.parseString(insertEdgesTsv)
      val tsvEdges = tsvs.map(tsv => Graph.toEdge(tsv)).flatten
      jsonEdges must containTheSameElementsAs(tsvEdges)
    }
  }
//  "Graph" should {
//    "test insert" in {
//      EdgeController.tryMutates(Json.parse(insertEdges), "insert")
//      val edgesWithRank = Graph.getEdgesSync(query)
//      edgesWithRank must have size(1)
//      val head = edgesWithRank.head.toArray
//      head must have size(4)
//      val (firstEdge, firstRAnk) = head(0)
//      firstEdge.tgtVertex.innerId must beEqualTo(InnerVal.withLong(103)) 
//      firstEdge.ts must beEqualTo (193829194)
//      
//      true
//    }
//  }
}