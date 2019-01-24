****************
Edge Queries
****************

S2Graph provides a query DSL which has been reported to have a pretty steep learning curve.
One tip is to try to understand each features by projecting it to that of a RDBMS such MySQL.
This doesn't work all the time, but there are many similarities between S2Graph and a conventional RDBMS.
For example, S2Graphs "getEdges" is used to fetch data and traverse multiple steps. This is very similar to the "SELECT" query in MySQL.

Traversing each step is similar to ``join`` operation in RDBMS. One thing to note here is that users must start their traverse from smaller set to terminate BFS early as soon as possible.
Another tip is to not be shy to ask! Ask any questions on our `mailing list`_. list or open an issue at our `github`_ with the problems that you're having with S2Graph.

.. _mailing list: https://groups.google.com/forum/#!forum/s2graph

.. _github: https://github.com/apache/incubator-s2graph


checkEdges - ``POST /graphs/checkEdges``
------------------------------------------

return edge for given vertex pair only if edge exist.
This is more ``general`` way to check edge existence between any given vertex pairs comparing using ``_to`` on query parameter


.. code:: bashn

   curl -XPOST localhost:9000/graphs/checkEdges -H 'Content-Type: Application/json' -d '
   [
     {"label": "talk_friend", "direction": "out", "from": 1, "to": 100},
     {"label": "talk_friend", "direction": "out", "from": 1, "to": 101}
   ]'


getEdges - ``POST /graphs/getEdges``
-----------------------------------------

Select edges with query.

**Duplicate Policy**

Here is a very basic query to fetch all edges that start from source vertex "101".

.. code:: bash

   curl -XPOST localhost:9000/graphs/getEdges -H 'Content-Type: Application/json' -d '
   {

       "srcVertices": [
           {
               "serviceName": "s2graph",
               "columnName": "user_id_test",
               "id": 101
           }
       ],
       "steps": [
           {
               "step": [
                   {
                       "label": "s2graph_label_test_weak",
                       "direction": "out",
                       "offset": 0,
                       "limit": 10,
                       "duplicate": "raw"
                   }
               ]
           }
       ]
   }'


``Notice the "duplicate" field``. If a target label's consistency level is ``weak`` and multiple edges exist with the same (from, to, label, direction) id, then the query is expect to have a policy for handling edge duplicates. S2Graph provides four duplicate policies on edges.


.. note::
   - raw: Allow duplicates and return all edges.
   - first: Return only the first edge if multiple edges exist. This is default.
   - countSum: Return only one edge, and return how many duplicates exist.
   - sum: Return only one edge, and return sum of the scores.


With duplicate "raw", there are actually three edges with the same (from, to, label, direction) id.

.. code:: bash

   {
     "size": 3,
     "degrees": [
         {
             "from": 101,
             "label": "s2graph_label_test_weak",
             "direction": "out",
             "_degree": 3
         }
     ],
     "results": [
         {
             "cacheRemain": -29,
             "from": 101,
             "to": "10",
             "label": "s2graph_label_test_weak",
             "direction": "out",
             "_timestamp": 6,
             "timestamp": 6,
             "score": 1,
             "props": {
                 "_timestamp": 6,
                 "time": -30,
                 "weight": 0,
                 "is_hidden": false,
                 "is_blocked": false
             }
         },
         {
             "cacheRemain": -29,
             "from": 101,
             "to": "10",
             "label": "s2graph_label_test_weak",
             "direction": "out",
             "_timestamp": 5,
             "timestamp": 5,
             "score": 1,
             "props": {
                 "_timestamp": 5,
                 "time": -10,
                 "weight": 0,
                 "is_hidden": false,
                 "is_blocked": false
             }
         },
         {
             "cacheRemain": -29,
             "from": 101,
             "to": "10",
             "label": "s2graph_label_test_weak",
             "direction": "out",
             "_timestamp": 4,
             "timestamp": 4,
             "score": 1,
             "props": {
                 "_timestamp": 4,
                 "time": 0,
                 "weight": 0,
                 "is_hidden": false,
                 "is_blocked": false
             }
         }
     ],
     "impressionId": 1972178414
   }

Duplicate "countSum" returns only one edge with the score sum of 3.

.. code:: bash

   {
     "size": 1,
     "degrees": [
         {
             "from": 101,
             "label": "s2graph_label_test_weak",
             "direction": "out",
             "_degree": 3
         }
     ],
     "results": [
         {
             "cacheRemain": -135,
             "from": 101,
             "to": "10",
             "label": "s2graph_label_test_weak",
             "direction": "out",
             "_timestamp": 4,
             "timestamp": 4,
             "score": 3,
             "props": {
                 "_timestamp": 4,
                 "time": 0,
                 "weight": 0,
                 "is_hidden": false,
                 "is_blocked": false
             }
         }
     ],
     "impressionId": 1972178414
  }


**Select Option Example**

In case you want to control the fields shown in the result edges, use the "select" option.

.. code:: bash

   {
     "select": ["from", "to", "label"],
     "srcVertices": [
         {
             "serviceName": "s2graph",
             "columnName": "user_id_test",
             "id": 101
         }
     ],
     "steps": [
         {
             "step": [
                 {
                     "label": "s2graph_label_test_weak",
                     "direction": "out",
                     "offset": 0,
                     "limit": 10,
                     "duplicate": "raw"
                 }
             ]
         }
     ]
   }

S2Graph will return only those fields in the result.

.. code:: bash

   {
     "size": 3,
     "degrees": [
         {
             "from": 101,
             "label": "s2graph_label_test_weak",
             "direction": "out",
             "_degree": 3
         }
     ],
     "results": [
         {
             "from": 101,
             "to": "10",
             "label": "s2graph_label_test_weak"
         },
         {
             "from": 101,
             "to": "10",
             "label": "s2graph_label_test_weak"
         },
         {
             "from": 101,
             "to": "10",
             "label": "s2graph_label_test_weak"
         }
     ],
     "impressionId": 1972178414
   }

Default value of the "select" option is an empty array which means that all edge fields are returned.


**groupBy Option Example**


Result edges can be grouped by a given field.

.. code:: bash

   {
      "select": ["from", "to", "label", "direction", "timestamp", "score", "time", "weight", "is_hidden", "is_blocked"],
      "groupBy": ["from", "to", "label"],
      "srcVertices": [
          {
              "serviceName": "s2graph",
              "columnName": "user_id_test",
              "id": 101
          }
      ],
      "steps": [
          {
              "step": [
                  {
                      "label": "s2graph_label_test_weak",
                      "direction": "out",
                      "offset": 0,
                      "limit": 10,
                      "duplicate": "raw"
                  }
              ]
          }
      ]
   }


You can see the result edges are grouped by their "from", "to", and "label" fields.


.. code:: bash

   {
     "size": 1,
     "results": [
         {
             "groupBy": {
                 "from": 101,
                 "to": "10",
                 "label": "s2graph_label_test_weak"
             },
             "agg": [
                 {
                     "from": 101,
                     "to": "10",
                     "label": "s2graph_label_test_weak",
                     "direction": "out",
                     "timestamp": 6,
                     "score": 1,
                     "props": {
                         "time": -30,
                         "weight": 0,
                         "is_hidden": false,
                         "is_blocked": false
                     }
                 },
                 {
                     "from": 101,
                     "to": "10",
                     "label": "s2graph_label_test_weak",
                     "direction": "out",
                     "timestamp": 5,
                     "score": 1,
                     "props": {
                         "time": -10,
                         "weight": 0,
                         "is_hidden": false,
                         "is_blocked": false
                     }
                 },
                 {
                     "from": 101,
                     "to": "10",
                     "label": "s2graph_label_test_weak",
                     "direction": "out",
                     "timestamp": 4,
                     "score": 1,
                     "props": {
                         "time": 0,
                         "weight": 0,
                         "is_hidden": false,
                         "is_blocked": false
                     }
                 }
             ]
         }
     ],
     "impressionId": 1972178414
   }


**filterOut option example**

You can also run two queries concurrently, and filter the result of one query with the result of the other.

.. code:: bash

   {
     "filterOutFields": ["_to"],
     "filterOut": {
         "srcVertices": [
             {
                 "serviceName": "s2graph",
                 "columnName": "user_id_test",
                 "id": 100
             }
         ],
         "steps": [
             {
                 "step": [
                     {
                         "label": "s2graph_label_test_weak",
                         "direction": "out",
                         "offset": 0,
                         "limit": 10,
                         "duplicate": "raw"
                     }
                 ]
             }
         ]
     },
     "srcVertices": [
         {
             "serviceName": "s2graph",
             "columnName": "user_id_test",
             "id": 101
         }
     ],
     "steps": [
         {
             "step": [
                 {
                     "label": "s2graph_label_test_weak",
                     "direction": "out",
                     "offset": 0,
                     "limit": 10,
                     "duplicate": "raw"
                 }
             ]
         }
     ]
   }

S2Graph will run two concurrent queries, one in the main step, and another in the filter out clause. Here is more practical example.


.. coce:: bash

   {
     "filterOut": {
       "srcVertices": [
         {
           "columnName": "uuid",
           "id": "Alec",
           "serviceName": "daumnews"
         }
       ],
       "steps": [
         {
           "step": [
             {
               "direction": "out",
               "label": "daumnews_user_view_news",
               "limit": 100,
               "offset": 0
             }
           ]
         }
       ]
     },
     "srcVertices": [
       {
         "columnName": "uuid",
         "id": "Alec",
         "serviceName": "daumnews"
       }
     ],
     "steps": [
       {
         "nextStepLimit": 10,
         "step": [
           {
             "direction": "out",
             "duplicate": "scoreSum",
             "label": "daumnews_user_view_news",
             "limit": 100,
             "offset": 0,
             "timeDecay": {
               "decayRate": 0.1,
               "initial": 1,
               "timeUnit": 86000000
             }
           }
         ]
       },
       {
         "nextStepLimit": 10,
         "step": [
           {
             "label": "daumnews_news_belongto_category",
             "limit": 1
           }
         ]
       },
       {
         "step": [
           {
             "direction": "in",
             "label": "daumnews_news_belongto_category",
             "limit": 10
           }
         ]
       }
     ]
   }



The main query from the above will traverse a graph of users and news articles as follows:

1. Fetch the list of news articles that user Alec read.
2. Get the categories of the result edges of step one.
3. Fetch other articles that were published in same category.


Meanwhile, Alec does not want to get articles that he already read. This can be taken care of with the following query in the filterOut option:
Articles that Alec has already read.


.. code:: bash

   {
     "size": 5,
     "degrees": [
         {
             "from": "Alec",
             "label": "daumnews_user_view_news",
             "direction": "out",
             "_degree": 6
         }
     ],
     "results": [
         {
             "cacheRemain": -19,
             "from": "Alec",
             "to": 20150803143507760,
             "label": "daumnews_user_view_news",
             "direction": "out",
             "_timestamp": 1438591888454,
             "timestamp": 1438591888454,
             "score": 0.9342237306639056,
             "props": {
                 "_timestamp": 1438591888454
             }
         },
         {
             "cacheRemain": -19,
             "from": "Alec",
             "to": 20150803150406010,
             "label": "daumnews_user_view_news",
             "direction": "out",
             "_timestamp": 1438591143640,
             "timestamp": 1438591143640,
             "score": 0.9333716513280771,
             "props": {
                 "_timestamp": 1438591143640
             }
         },
         {
             "cacheRemain": -19,
             "from": "Alec",
             "to": 20150803144908340,
             "label": "daumnews_user_view_news",
             "direction": "out",
             "_timestamp": 1438581933262,
             "timestamp": 1438581933262,
             "score": 0.922898833570944,
             "props": {
                 "_timestamp": 1438581933262
             }
         },
         {
             "cacheRemain": -19,
             "from": "Alec",
             "to": 20150803124627492,
             "label": "daumnews_user_view_news",
             "direction": "out",
             "_timestamp": 1438581485765,
             "timestamp": 1438581485765,
             "score": 0.9223930035297659,
             "props": {
                 "_timestamp": 1438581485765
             }
         },
         {
             "cacheRemain": -19,
             "from": "Alec",
             "to": 20150803113311090,
             "label": "daumnews_user_view_news",
             "direction": "out",
             "_timestamp": 1438580536376,
             "timestamp": 1438580536376,
             "score": 0.9213207756669546,
             "props": {
                 "_timestamp": 1438580536376
             }
         }
     ],
     "impressionId": 354266627
   }


Without "filterOut"

.. code:: bash

  {
    "size": 2,
    "degrees": [
        {
            "from": 1028,
            "label": "daumnews_news_belongto_category",
            "direction": "in",
            "_degree": 2
        }
    ],
    "results": [
        {
            "cacheRemain": -33,
            "from": 1028,
            "to": 20150803105805092,
            "label": "daumnews_news_belongto_category",
            "direction": "in",
            "_timestamp": 1438590169146,
            "timestamp": 1438590169146,
            "score": 0.9342777143725886,
            "props": {
                "updateTime": 20150803172249144,
                "_timestamp": 1438590169146
            }
        },
        {
            "cacheRemain": -33,
            "from": 1028,
            "to": 20150803143507760,
            "label": "daumnews_news_belongto_category",
            "direction": "in",
            "_timestamp": 1438581548486,
            "timestamp": 1438581548486,
            "score": 0.9342777143725886,
            "props": {
                "updateTime": 20150803145908490,
                "_timestamp": 1438581548486
            }
        }
    ],
    "impressionId": -14034523
  }


with "filterOut"


.. code:: bash

   {
     "size": 1,
     "degrees": [],
     "results": [
         {
             "cacheRemain": 85957406,
             "from": 1028,
             "to": 20150803105805092,
             "label": "daumnews_news_belongto_category",
             "direction": "in",
             "_timestamp": 1438590169146,
             "timestamp": 1438590169146,
             "score": 0.9343106784173475,
             "props": {
                 "updateTime": 20150803172249144,
                 "_timestamp": 1438590169146
             }
         }
     ],
     "impressionId": -14034523
   }


Note that article ``20150803143507760`` has been filtered out.


**nextStepLimit Example**

S2Graph provides step-level aggregation so that users can take the top K items from the aggregated results.

**nextStepThreshold Example**

**sample Example**

.. code:: bash

   curl -XPOST localhost:9000/graphs/getEdges -H 'Content-Type: Application/json' -d '
   {
     "srcVertices": [{"serviceName": "s2graph", "columnName": "account_id", "id":1}],
     "steps": [
       {"sample":2,"step": [{"label": "graph_test", "direction": "out", "offset": 0, "limit": 10, "scoring": {"time": 1, "weight": 1}}]}
     ]
   }


**transform Example**

With typical two-step query, S2Graph will start the second step from the "_to" (vertex id) values of the first steps' result. With the "transform" option, you can actually use any single field from the result edges' properties of step one.

Add a "transform" option to the query from example 1.

.. code:: bash

   {
     "select": [],
     "srcVertices": [
         {
             "serviceName": "s2graph",
             "columnName": "user_id_test",
             "id": 101
         }
     ],
     "steps": [
         {
             "step": [
                 {
                     "label": "s2graph_label_test_weak",
                     "direction": "out",
                     "offset": 0,
                     "limit": 10,
                     "duplicate": "raw",
                     "transform": [
                         ["_to"],
                         ["time.$", "time"]
                     ]
                 }
             ]
         }
     ]
   }

Note that we have six resulting edges. We have two transform rules, the first one simply fetches edges with their target vertex IDs (such as "to": "10"), and the second rule will fetch the same edges but with the "time" values replacing vertex IDs (such as "to": "to": "time.-30").

.. code:: bash

   {
     "size": 6,
     "degrees": [
         {
             "from": 101,
             "label": "s2graph_label_test_weak",
             "direction": "out",
             "_degree": 3
         },
         {
             "from": 101,
             "label": "s2graph_label_test_weak",
             "direction": "out",
             "_degree": 3
         }
     ],
     "results": [
         {
             "cacheRemain": -8,
             "from": 101,
             "to": "10",
             "label": "s2graph_label_test_weak",
             "direction": "out",
             "_timestamp": 6,
             "timestamp": 6,
             "score": 1,
             "props": {
                 "_timestamp": 6,
                 "time": -30,
                 "weight": 0,
                 "is_hidden": false,
                 "is_blocked": false
             }
         },
         {
             "cacheRemain": -8,
             "from": 101,
             "to": "time.-30",
             "label": "s2graph_label_test_weak",
             "direction": "out",
             "_timestamp": 6,
             "timestamp": 6,
             "score": 1,
             "props": {
                 "_timestamp": 6,
                 "time": -30,
                 "weight": 0,
                 "is_hidden": false,
                 "is_blocked": false
             }
         },
         {
             "cacheRemain": -8,
             "from": 101,
             "to": "10",
             "label": "s2graph_label_test_weak",
             "direction": "out",
             "_timestamp": 5,
             "timestamp": 5,
             "score": 1,
             "props": {
                 "_timestamp": 5,
                 "time": -10,
                 "weight": 0,
                 "is_hidden": false,
                 "is_blocked": false
             }
         },
         {
             "cacheRemain": -8,
             "from": 101,
             "to": "time.-10",
             "label": "s2graph_label_test_weak",
             "direction": "out",
             "_timestamp": 5,
             "timestamp": 5,
             "score": 1,
             "props": {
                 "_timestamp": 5,
                 "time": -10,
                 "weight": 0,
                 "is_hidden": false,
                 "is_blocked": false
             }
         },
         {
             "cacheRemain": -8,
             "from": 101,
             "to": "10",
             "label": "s2graph_label_test_weak",
             "direction": "out",
             "_timestamp": 4,
             "timestamp": 4,
             "score": 1,
             "props": {
                 "_timestamp": 4,
                 "time": 0,
                 "weight": 0,
                 "is_hidden": false,
                 "is_blocked": false
             }
         },
         {
             "cacheRemain": -8,
             "from": 101,
             "to": "time.0",
             "label": "s2graph_label_test_weak",
             "direction": "out",
             "_timestamp": 4,
             "timestamp": 4,
             "score": 1,
             "props": {
                 "_timestamp": 4,
                 "time": 0,
                 "weight": 0,
                 "is_hidden": false,
                 "is_blocked": false
             }
         }
     ],
     "impressionId": 1972178414
   }


**Two-Step Traversal Example**

The following query will fetch a user's (id 1) friends of friends by chaining multiple steps:


.. code:: bash

   {
     "srcVertices": [{"serviceName": "s2graph", "columnName": "account_id", "id":1}],
     "steps": [
       {
           "step": [
             {"label": "friends", "direction": "out", "limit": 100}
           ]
       },
       {
           "step": [
             {"label": "friends", "direction": "out", "limit": 10}
           ]
       }
     ]
   }'

**Three-Step Traversal Example**

Add more steps for wider traversals. Be gentle on the limit options since the number of visited edges will increase exponentially and become very heavy on the system.

**More examples**

Example 1. From label "graph_test", select the first 100 edges that start from vertex "account_id = 1", with default sorting.

.. code:: bash


   curl -XPOST localhost:9000/graphs/getEdges -H 'Content-Type: Application/json' -d '
   {
       "srcVertices": [{"serviceName": "s2graph", "columnName": "account_id", "id":1}],
       "steps": [
         [{"label": "graph_test", "direction": "out", "offset": 0, "limit": 100
         }]
       ]
   }'

Example 2. Now select between the 50th and 100th edges from the same query.

.. code:: bash

   curl -XPOST localhost:9000/graphs/getEdges -H 'Content-Type: Application/json' -d '
   {
       "srcVertices": [{"serviceName": "s2graph", "columnName": "account_id", "id":1}],
       "steps": [
         [{"label": "graph_test", "direction": "in", "offset": 50, "limit": 50}]
       ]
   }'

Example 3. Now add a time range filter so that you will only get the edges that were inserted between 1416214118000 and 1416300000000.

.. code:: bash

   curl -XPOST localhost:9000/graphs/getEdges -H 'Content-Type: Application/json' -d '
   {
       "srcVertices": [{"serviceName": "s2graph", "columnName": "account_id", "id":1}],
       "steps": [
         [{"label": "graph_test", "direction": "in", "offset": 50, "limit": 50, "duration": {"from": 1416214118000, "to": 1416300000000}]
       ]
   }'

Example 4. Now add scoring rule to sort the result by indexed properties "time" and "weight", with weights of 1.5 and 10, respectively.

.. code:: bash

   curl -XPOST localhost:9000/graphs/getEdges -H 'Content-Type: Application/json' -d '
   {
       "srcVertices": [{"serviceName": "s2graph", "columnName": "account_id", "id":1}],
       "steps": [
         [{"label": "graph_test", "direction": "in", "offset": 50, "limit": 50, "duration": {"from": 1416214118000, "to": 1416214218000}, "scoring": {"time": 1.5, "weight": 10}]
       ]
   }'


Example 5. Make a two-step query to fetch friends of friends of a user "account_id = 1". (Limit the first step by 10 friends and the second step by 100.)

.. code:: bash

   curl -XPOST localhost:9000/graphs/getEdges -H 'Content-Type: Application/json' -d '
   {
       "srcVertices": [{"serviceName": "s2graph", "columnName": "account_id", "id":1}],
       "steps": [
         [{"label": "friends", "direction": "out", "limit": 100}],
         [{"label": "friends", "direction": "out", "limit": 10}]
       ]
   }'


Example 6. Make a two-step query to fetch the music playlist of the friends of user "account_id = 1". Limit the first step by 10 friends and the second step by 100 tracks.)

.. code:: bash

   curl -XPOST localhost:9000/graphs/getEdges -H 'Content-Type: Application/json' -d '
   {
       "srcVertices": [{"serviceName": "s2graph", "columnName": "account_id", "id":1}],
       "steps": [
         [{"label": "talk_friend", "direction": "out", "limit": 100}],
         [{"label": "play_music", "direction": "out", "limit": 10}]
       ]
   }'


Example 7. Query the friends of user "account_id = 1" who played the track "track_id = 200".

.. code:: bash

   curl -XPOST localhost:9000/graphs/getEdges -H 'Content-Type: Application/json' -d '
   {
       "srcVertices": [{"serviceName": "s2graph", "columnName": "account_id", "id":1}],
       "steps": [
         [{"label": "talk_friend", "direction": "out", "limit": 100}],
         [{"label": "play_music", "direction": "out", "_to": 200}]
       ]
   }'
