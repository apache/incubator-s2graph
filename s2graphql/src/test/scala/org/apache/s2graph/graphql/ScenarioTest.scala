/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.s2graph.graphql

import com.typesafe.config.ConfigFactory
import org.scalatest._
import play.api.libs.json._
import sangria.execution.ValidationError
import sangria.macros._
import sangria.parser.QueryParser

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
          """
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

      it("should add props(gender) to serviceColumn 'user'") {
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
              Services(name: kakao) {
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
                "Services": [{
                  "name": "kakao",
                  "serviceColumns": [{
                    "name": "user",
                    "props": [
                      { "name": "age", "dataType": "int" },
                      { "name": "gender", "dataType": "string" }
                    ]
                  }]
                }]
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
                  dataType: double
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
              Labels(name: friends) {
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
        val expected = Json.parse(
          """
          {
          	"data": {
          		"Management": {
          			"Labels": [{
          				"name": "friends",
          				"props": [{
          					"name": "score",
          					"dataType": "double"
          				}]
          			}]
          		}
          	}
          }
          """)

        actual shouldBe expected
      }
    }

    describe("Add vertex to kakao.user' and fetch ") {
      it("should add vertices: daewon(age: 20, gender: M), shon(age: 19), gender: F) to kakao.user") {
        val query =
          graphql"""

          mutation {
            addVertex(
              kakao: {
                user: [{
                  id: "daewon"
                  age: 20
                  gender: "M"
                },
                {
                  id: "shon"
                  age: 19
                  gender: "F"
                },
                {
                  id: "rain"
                  age: 21
                  gender: "T"
                }]
              }) {
             isSuccess
           }
          }
        """

        val actual = testGraph.queryAsJs(query)
        val expected = Json.parse(
          """
          {
            "data": {
          	"addVertex": [{
          	  "isSuccess": true
          	}, {
          	  "isSuccess": true
          	}, {
          	  "isSuccess": true
          	}
          	]}
          }
        """)

        actual shouldBe expected
      }

      it("should fetch vertices: daewon(age: 20, gender: M), shon(age: 19), gender: F) from kakao.user") {
        val query =
          graphql"""

          query FetchVertices {
            kakao {
              user(ids: ["daewon", "shon"]) {
                id
                age
                gender
                props {
                  age
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
          		"kakao": {
          			"user": [{
          				"id": "daewon",
          				"age": 20,
                  "gender": "M",
                  "props": {
                    "age": 20
                  }
          			}, {
          				"id": "shon",
          				"age": 19,
                  "gender": "F",
                  "props": {
                    "age": 19
                  }
          			}]
          		}
          	}
          }
          """)

        actual shouldBe expected
      }

      it("should fetch vertices: offset, limit") {
        val query =
          graphql"""

          query FetchVertices {
            kakao {
              user(search: "gender: M OR gender: F or gender: T", offset: 1, limit: 2) {
                id
                age
                gender
                props {
                  age
                }
              }
            }
          }
        """

        val actual = testGraph.queryAsJs(query)
        val expected = 2

        // The user order may vary depending on the indexProvider(es, lucene).
        (actual \\ "gender").size shouldBe expected
      }

      it("should fetch vertices using VertexIndex: 'gender in (F, M)') from kakao.user") {
        def query(search: String) = {
          QueryParser.parse(
            s"""
            query FetchVertices {
              kakao {
                user(search: "${search}", ids: ["rain"]) {
                  id
                  age
                  gender
                }
              }
            }""").get
        }

        val actualGender = testGraph.queryAsJs(query("gender: M OR gender: F"))
        val expected = Json.parse(
          """
          {
          	"data": {
          		"kakao": {
          			"user": [
          			  {"id": "rain", "age": 21, "gender": "T"},
          			  {"id": "daewon", "age": 20, "gender": "M"},
                  {"id": "shon", "age": 19, "gender": "F"}
                ]
          		}
          	}
          }
          """)

        actualGender shouldBe expected

        val actualAge = testGraph.queryAsJs(query("age: 19 OR age: 20"))
        actualAge shouldBe expected

        val actualAgeBetween = testGraph.queryAsJs(query("age: [10 TO 20]"))
        actualAgeBetween shouldBe expected
      }
    }

    describe("Add edge to label 'friends' and fetch ") {
      it("should add edges: daewon -> shon(score: 2) to friends") {
        val query =
          graphql"""

        mutation {
          addEdge(
            friends: {
              from: "daewon"
              to: "shon"
              score: 0.9
            }
          ) {
           isSuccess
         }
        }
        """

        val actual = testGraph.queryAsJs(query)

        val expected = Json.parse(
          """
        {
        	"data": {
        		"addEdge": [{
        			"isSuccess": true
        		}]
        	}
        }
        """)

        actual shouldBe expected
      }

      it("should fetch edges: friends of kakao.user(daewon) ") {
        val query =
          graphql"""

        query {
          kakao {
            user(id: "daewon") {
              id
              friends(direction: out) {
                props {
                  score
                }
                score # shorcut score props
                user {
                  id
                  age
                  friends(direction: in) {
                    user {
                      id
                      age
                    }
                    direction
                  }
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
          "data" : {
            "kakao" : {
              "user" : [ {
                "id" : "daewon",
                "friends" : [ {
                  "props" : {
                    "score" : 0.9
                  },
                  "score" : 0.9,
                  "user" : {
                    "id" : "shon",
                    "age" : 19,
                    "friends" : [ {
                      "user" : {
                        "id" : "daewon",
                        "age" : 20
                      },
                      "direction" : "in"
                    } ]
                  }
                } ]
              } ]
            }
          }
        }
        """)

        actual shouldBe expected
      }
    }

    describe("Management: Delete label, service column") {

      it("should delete label: 'friends' and serviceColumn: 'user' on kakao") {
        val query =
          graphql"""

        mutation {
          Management {
            deleteLabel(name: friends) {
              isSuccess
            }

            deleteServiceColumn(service: {
              kakao: {
                columnName: user
              }
            }) {
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
          	  "deleteLabel": {
                "isSuccess": true
          	  },
              "deleteServiceColumn": {
                "isSuccess": true
          	  }
          	}
          }
        }
      """)

        actual shouldBe expected
      }

      it("should fetch failed label: 'friends' and serviceColumn: 'user'") {
        val query =
          graphql"""

          query {
            Management {
              Labels(name: friends) {
                name
                props {
                  name
                  dataType
                }
              }
            }
          }
          """

        // label 'friends' was deleted
        assertThrows[ValidationError] {
          testGraph.queryAsJs(query)
        }

        val queryServiceColumn =
          graphql"""

          query {
            Management {
             Services(name: kakao) {
                serviceColumns {
                  name
                }
              }
            }
          }
          """

        // serviceColumn 'user' was deleted
        val actual = testGraph.queryAsJs(queryServiceColumn)
        val expected = Json.parse(
          """
        {
        	"data": {
        		"Management": {
        			"Services": [{
        				"serviceColumns": []
        			}]
        		}
        	}
        }
        """)

        actual shouldBe expected
      }
    }
  }
}
