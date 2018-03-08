package org.apache.s2graph.graphql

import com.typesafe.config.ConfigFactory
import org.scalatest._
import play.api.libs.json._
import sangria.macros._

class ScenarioTest extends FunSpec with Matchers with BeforeAndAfterAll {
  var testGraph: TestGraph = _

  override def beforeAll = {
    val config = ConfigFactory.load()
    testGraph = new EmptyGraph(config)
    testGraph.open()
  }

  override def afterAll(): Unit = {
    testGraph.cleanup()
  }

  info("Use the GraphQL API to create basic friendships.")

  describe("Create schema using Management API") {
    describe("Create and query Service, ServiceColumn, ColumnMeta(Props)") {
      it("should create service: 'kakao'") {
        val query =
          graphql"""

        mutation {
          Management {
            createService(
              name: "kakao"
              compressionAlgorithm: gz
            ) {
              isSuccess
            }
          }
        }

        """

        val actual = testGraph.queryAsJs(query)
        val expected = Json.parse(
          """
          {
          	"data": {
          		"Management": {
          			"createService": {
          				"isSuccess": true
          			}
          		}
          	}
          }
          """.stripMargin
        )

        actual shouldBe expected
      }

      it("should create serviceColumn to Service 'kakao'") {
        val query =
          graphql"""

        mutation {
          Management {
            createServiceColumn (
              serviceName: kakao
              columnName: "user"
              columnType: string
              props: {
                name: "age"
                dataType: int
                defaultValue: "0"
                storeInGlobalIndex: true
              }
            ) {
              isSuccess
              object {
                name
                props {
                  name
                }
              }
            }
          }
        }

        """

        val actual = testGraph.queryAsJs(query)
        val expected = Json.parse(
          """
          {
           	"data": {
           		"Management": {
           			"createServiceColumn": {
           				"isSuccess": true,
           				"object": {
           					"name": "user",
           					"props": [{
           						"name": "age"
           					}]
           				}
           			}
           		}
           	}
         }
         """)

        actual shouldBe expected
      }

      it("should add props to serviceColumn 'user'") {
        val query =
          graphql"""

        mutation {
          Management {
            addPropsToServiceColumn(
              service: {
                kakao: {
                  columnName: user
                  props: {
                    name: "gender"
                    dataType: string
                    defaultValue: ""
                    storeInGlobalIndex: true
                  }
                }
              }
            ) {
              isSuccess
            }
          }
        }
        """

        val actual = testGraph.queryAsJs(query)
        val expected = Json.parse(
          """
           {
              "data": {
                "Management": {
                  "addPropsToServiceColumn": {
                    "isSuccess": true
                  }
                }
              }
           }

          """)

        actual shouldBe expected
      }

      it("should fetch service: 'kakao' with serviceColumn: 'user' with props: ['age', 'gender']") {
        val query =
          graphql"""

        query {
          Management {
            Service(name: kakao) {
              name
              serviceColumns {
                name
                props {
                  name
                  dataType
                }
              }
            }
          }
        }

        """

        val actual = testGraph.queryAsJs(query)
        val expected = Json.parse(
          """
          {
            "data": {
              "Management": {
                "Service": {
                  "name": "kakao",
                  "serviceColumns": [{
                    "name": "user",
                    "props": [
                      { "name": "age", "dataType": "int" },
                      { "name": "gender", "dataType": "string" }
                    ]
                  }]
                }
              }
            }
          }
          """)

        actual shouldBe expected
      }
    }

    describe("Create and query Label, LabelMeta(Props)") {
      it("should create label: 'friends'") {
        val query =
          graphql"""

        mutation {
          Management {
            createLabel(
              name: "friends"
              sourceService: {
                kakao: {
                  columnName: user
                }
              }
              targetService: {
                kakao: {
                  columnName: user
                }
              }
            ) {
              isSuccess
            }
          }
        }

        """

        val actual = testGraph.queryAsJs(query)
        val expected = Json.parse(
          """
          {
          	"data": {
          		"Management": {
          			"createLabel": {
          				"isSuccess": true
          			}
          		}
          	}
          }
        """)

        actual shouldBe expected
      }

      it("should add props to label 'friends'") {
        val query =
          graphql"""

          mutation {
            Management {
              addPropsToLabel(
                labelName: friends
                props: {
                  name: "score"
                  dataType: float
                  defaultValue: "0"
                  storeInGlobalIndex: true
               }
             ) {
                isSuccess
              }
            }
          }
        """

        val actual = testGraph.queryAsJs(query)
        val expected = Json.parse(
          """
         {
         	"data": {
         		"Management": {
         			"addPropsToLabel": {
         				"isSuccess": true
         			}
         		}
         	}
         }
        """)

        actual shouldBe expected
      }

      it("should fetch label: 'friends' with props: ['score']") {
        val query =
          graphql"""

        query {
          Management {
            Label(name: friends) {
              name
              props {
                name
                dataType
              }
            }
          }
        }

        """

        val actual = testGraph.queryAsJs(query)
        println(actual)
        val expected = Json.parse(
          """
         {
         	"data": {
         		"Management": {
         			"Label": {
         				"name": "friends",
         				"props": [{
         					"name": "score",
         					"dataType": "float"
         				}]
         			}
         		}
         	}
         }
         """)

        actual shouldBe expected
      }
    }
  }
}
