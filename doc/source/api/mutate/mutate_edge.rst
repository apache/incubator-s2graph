****************
Manage Edges
****************

An ``Edge`` represents a relation between two vertices, with properties according to the schema defined in its label.

Edge Fields
---------------

The following fields need to be specified when inserting an edge, and are returned when queried on edges.

.. csv-table:: Option
   :header: "Field Name", "Definition", "Data Type", "Example", "Note"
   :widths: 15, 30, 30, 30, 30

   "**timestamp**",	"Issue time of request", "Long", "1430116731156", "Required. Unix Epoch time in milliseconds. S2Graph TTL and timestamp unit is **milliseconds**"
   "operation", "One of insert, delete, update, or increment", "String", "'i', 'insert'", "Required only for bulk operations. Aliases are also available: i (insert), d (delete), u (update), in (increment). Default is insert"
   "from", "Id of source vertex", "Long/String",	"1", "Required. Use long if possible. **Maximum string byte-size is 249**"
   "to", "Id of target vertex", "Long/String", "101", "Required. Use long if possible. **Maximum string byte-size is 249**"
   "label",	"Label name",	"String",	"graph_test", "Required"
   "direction",	"Direction of the edge. Should be one of **out/ in/ undirected**", "String", "out", "Required. Alias are also available: o (out), i (in), u (undirected)"
   "props",	"Additional properties of the edge", "JSON (dictionary)",	"{""timestamp"": 1417616431, ""affinity_score"":10, ""is_hidden"": false, ""is_valid"": true}", "Required. **If in indexed properties isn't given, default values will be added**"


Basic Edge Operations
--------------------------

In S2Graph, an Edge supports five different operations.

- ``insert``: Create new edge.
- ``delete``: Delete existing edge.
- ``update``: Update existing edge`s state.
- ``increment``: Increment existing edge`s state.
- ``deleteAll``: Delete all adjacent edges from certain source vertex. (Available for strong consistency only)

Edge operations work differently depending on the target label`s consistency level.

For a better understanding, please take a look at the following test cases.

Create 2 different labels, one of each consistencyLevels.

- s2graph_label_test (strong)
- s2graph_label_test_weak (weak)

Then insert a same set of edges to each labels and query them as follows.

**strong consistency**

.. code:: bash

  curl -XPOST localhosnt:9000/graphs/edges/insert -H 'Content-Type: Application/json' -d '
  [
    {"timestamp": 1, "from": 101, "to": 10, "label": "s2graph_label_test", "props": {"time": 0}},
    {"timestamp": 2, "from": 101, "to": 10, "label": "s2graph_label_test", "props": {"time": -10}},
    {"timestamp": 3, "from": 101, "to": 10, "label": "s2graph_label_test", "props": {"time": -30}}
  ]'


Note that only one edge exist between (101, 10, s2graph_label_test, out).

.. code:: json

   {
     "size": 1,
     "degrees": [
       {
         "from": 101,
          "label": "s2graph_label_test",
          "direction": "out",
          "_degree": 1
       }
     ],
     "results": [
       {
         "cacheRemain": -20,
         "from": 101,
          "to": 10,
          "label": "s2graph_label_test",
          "direction": "out",
          "_timestamp": 3,
          "timestamp": 3,
          "score": 1,
          "props": {
            "_timestamp": 3,
            "time": -30,
            "weight": 0,
            "is_hidden": false,
            "is_blocked": false
          }
        }
     ],
     "impressionId": -1650835965
   }

**weak consistency**

.. code:: bash

   curl -XPOST localhost:9000/graphs/edges/insert -H 'Content-Type: Application/json' -d '
   [
     {"timestamp": 1, "from": 101, "to": 10, "label": "s2graph_label_test_weak", "props": {"time": 0}},
     {"timestamp": 2, "from": 101, "to": 10, "label": "s2graph_label_test_weak", "props": {"time": -10}},
     {"timestamp": 3, "from": 101, "to": 10, "label": "s2graph_label_test_weak", "props": {"time": -30}}
   ]'

This time there are ``three edges`` between (101, 10, s2graph_label_test_weak, out).

.. code:: json

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
         "cacheRemain": -148,
         "from": 101,
         "to": "10",
         "label": "s2graph_label_test_weak",
         "direction": "out",
         "_timestamp": 3,
         "timestamp": 3,
         "score": 1,
         "props": {
           "_timestamp": 3,
           "time": -30,
           "weight": 0,
           "is_hidden": false,
           "is_blocked": false
         }
       },
       {
         "cacheRemain": -148,
         "from": 101,
         "to": "10",
         "label": "s2graph_label_test_weak",
         "direction": "out",
         "_timestamp": 2,
         "timestamp": 2,
         "score": 1,
         "props": {
           "_timestamp": 2,
           "time": -10,
           "weight": 0,
           "is_hidden": false,
           "is_blocked": false
         }
       },
       {
         "cacheRemain": -148,
         "from": 101,
         "to": "10",
         "label": "s2graph_label_test_weak",
         "direction": "out",
         "_timestamp": 1,
         "timestamp": 1,
         "score": 1,
         "props": {
           "_timestamp": 1,
           "time": 0,
           "weight": 0,
           "is_hidden": false,
           "is_blocked": false
         }
       }
     ],
      "impressionId": 1972178414
    }


Strong Consistency
---------------------

Insert - ``POST /mutate/edge/insert``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A unique edge is identified by a combination of (from, to, label, direction). For insert operations, S2Graph first checks if an edge with same (from, to, label, direction) information exists. If there is an existing edge, then insert will work as ``update``. See above example.

Delete - ``POST /mutate/edge/delete``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


For edge deletion, again, S2Graph looks for a unique edge with (from, to, label, direction). However, this time it checks the timestamp of the delete request and the existing edge. The timestamp on the delete request ``must be larger than that on the existing edge`` or else the request will be ignored. If everything is well, the edge will be deleted. Also note that no props information is necessary for a delete request on a strongly consistent label since there will be only one edge with edge`s unique id(from, to, label, direction).

.. code:: bash

   curl -XPOST localhost:9000/mutate/edge/delete -H 'Content-Type: Application/json' -d '
   [
     {"timestamp": 10, "from": 101, "to": 10, "label": "s2graph_label_test"}
   ]'

Update - ``POST /mutate/edge/update``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

What an update operation does to a strongly consistent label is identical to an insert.


.. code:: bash

   curl -XPOST localhost:9000/mutate/edge/update -H 'Content-Type: Application/json' -d '
   [
     {"timestamp": 10, "from": 101, "to": 10, "label": "s2graph_label_test", "props": {"time": 100, "weight": -10}}
   ]'


Increment - ``POST /mutate/edge/increment``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Works like update, other than it returns the incremented value and not the old value.

.. code:: bash

   curl -XPOST localhost:9000/mutate/edge/increment -H 'Content-Type: Application/json' -d '
   [
     {"timestamp": 10, "from": 101, "to": 10, "label": "s2graph_label_test", "props": {"time": 100, "weight": -10}}
   ]'

Delete All - ``POST /mutate/edge/deleteAll``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Delete all adjacent edges to the source vertex. ``Please note that edges with both in and out directions will be deleted``

.. code:: bash

   curl -XPOST localhost:9000/mutate/edge/deleteAll -H 'Content-Type: Application/json' -d '
   [
     {"ids" : [101], "label":"s2graph_label_test", "direction": "out", "timestamp":1417616441000}
   ]'


Weak Consistency
-----------------


Insert ``POST /mutate/edge/insert``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

S2Graph ``does not look`` for a unique edge defined by (from, to, label, direction). It simply stores a new edge according to the request. No read, no consistency check. Note that this difference allows multiple edges with same (from, to, label, direction) id.

Delete - ``POST /graphs/edges/delete``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For deletion on weakly consistent edges, first, S2Graph fetches existing edges from storage. Then, on each resulting edges, fires the actual delete operations.

.. code:: bash

   curl -XPOST localhost:9000/graphs/edges/delete -H 'Content-Type: Application/json' -d '
   [
     {
       "cacheRemain": -148,
       "from": 101,
       "to": "10",
       "label": "s2graph_label_test_weak",
       "direction": "out",
       "_timestamp": 3,
       "timestamp": 3,
       "score": 1,
       "props": {
         "_timestamp": 3,
         "time": -30,
         "weight": 0,
         "is_hidden": false,
         "is_blocked": false
       }
     },
     {
       "cacheRemain": -148,
       "from": 101,
       "to": "10",
       "label": "s2graph_label_test_weak",
       "direction": "out",
       "_timestamp": 2,
       "timestamp": 2,
       "score": 1,
       "props": {
         "_timestamp": 2,
         "time": -10,
         "weight": 0,
         "is_hidden": false,
         "is_blocked": false
       }
     },
     {
       "cacheRemain": -148,
       "from": 101,
       "to": "10",
       "label": "s2graph_label_test_weak",
       "direction": "out",
       "_timestamp": 1,
       "timestamp": 1,
       "score": 1,
       "props": {
         "_timestamp": 1,
         "time": 0,
         "weight": 0,
         "is_hidden": false,
         "is_blocked": false
       }
     }
   ]'

Update - ``POST /mutate/edge/update``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Like insert, S2Graph ``does not check`` for uniqueness. Update requires a pre-fetch of existing edges, similar to delete. Props of the resulting edges will be updated.

Increment - ``POST /mutate/edge/increment``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For increment, S2Graph also ``does not check`` for uniqueness. Update requires a pre-fetch of existing edges, similar to delete. Props of the resulting edges will be incremented.

Delete All - ``POST /mutate/edge/deleteAll``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Identical to strong consistency.

.. code:: bash

   curl -XPOST localhost:9000/mutate/edge/deleteAll -H 'Content-Type: Application/json' -d '
   [
     {"ids" : [101], "label":"s2graph_label_test", "direction": "out", "timestamp":1417616441}
   ]'
