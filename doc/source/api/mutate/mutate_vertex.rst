****************************
Manage Vertices (Optional)
****************************

Vertices are the two end points of an edge, and logically stored in columns of a service. If your use case requires storing metadata corresponding to vertices rather than edges, there are operations available on vertices as well.


Vertex Fields
----------------

Unlike edges and their labels, properties of a vertex are not indexed nor require a predefined schema. The following fields are used when operating on vertices.


.. csv-table:: Option
   :header: "Field Name", "Definition", "Data Type", "Example", "Note"
   :widths: 15, 30, 30, 30, 30

   "timestamp",	"Issue time of request", "Long", "1430116731156", "Required. Unix Epoch time in **milliseconds**"
   "operation",	"One of insert, delete, update, increment", "String", "i, insert", "Required only for bulk operations. Alias are also available: i (insert), d (delete), u (update), in (increment). Default is insert."
   "**serviceName**", "Corresponding service name", "String", "kakaotalk/kakaogroup", "Required"
   "**columnName**", "Corresponding column name", "String", "user_id", "Required"
   "id", "Unique identifier of vertex", "Long/String", "101", "Required. Use Long if possible"
   "**props**", "Additional properties of vertex", "JSON (dictionary)", "{""is_active_user"": true, ""age"":10, ""gender"": ""F"", ""country_iso"": ""kr""}", "Required"




Basic Vertex Operations
--------------------------

Insert - ``POST /mutate/vertex/insert/:serviceName/:columnName``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


.. code:: bash

   curl -XPOST localhost:9000/mutate/vertex/insert/s2graph/account_id -H 'Content-Type: Application/json' -d '
   [
     {"id":1,"props":{"is_active":true, "talk_user_id":10},"timestamp":1417616431000},
     {"id":2,"props":{"is_active":true, "talk_user_id":12},"timestamp":1417616431000},
     {"id":3,"props":{"is_active":false, "talk_user_id":13},"timestamp":1417616431000},
     {"id":4,"props":{"is_active":true, "talk_user_id":14},"timestamp":1417616431000},
     {"id":5,"props":{"is_active":true, "talk_user_id":15},"timestamp":1417616431000}
   ]'


Delete - ``POST /mutate/vertex/delete/:serviceName/:columnName``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: bash

   curl -XPOST localhost:9000/mutate/vertex/delete/s2graph/account_id -H 'Content-Type: Application/json' -d '
   [
     {"id":1,"timestamp":1417616431001},
     {"id":2,"timestamp":1417616431002}
   ]'


This operation will delete only the vertex data of a specified column and will not delete any edges connected to those vertices.
Also important thing is timestamp. vertex will be deleted only if delete requests ``timestamp is larger than all of vertexs`` property`s timestamp.

following example shows the difference.

.. code:: bash

   curl -XPOST localhost:9000/mutate/vertex/insert/s2graph_test/account_id -H 'Content-Type: Application/json' -d '
   [
     {"id":1,"props":{"is_active":true, "talk_user_id":10},"timestamp":1417616431000},
     {"id":1,"props":{"talk_user_id":20},"timestamp":1417616431002}
   ]'

if user request delete(ts) on vertex like below then vertex would not be deleted, but only properties on this vertex that is updated before ts will be deleted.

.. code:: bash

   curl -XPOST localhost:9000/mutate/vertex/delete/s2graph_test/account_id -H 'Content-Type: Application/json' -d '
   [
     {"id":1,"timestamp":1417616431001}
   ]'


then result still have vertex with property that is updated with larger timestamp.


.. code:: bash

   curl -XPOST localhost:9000/graphs/getVertices -H 'Content-Type: Application/json' -d '
   [
      {"serviceName": "s2graph_test", "columnName": "account_id", "ids": [1]}
   ]'


   # result
   {
     "serviceName": "s2graph_test",
     "columnName": "account_id",
     "id": 1,
     "props": {
       "talk_user_id": 20
     },
     "timestamp": 1417616431002
   }


**Important notes**

.. note::
   This means that edges returned by a query can contain deleted vertices. Clients are responsible for checking validity of the vertices.

Delete All - ``POST /mutate/vertex/deleteAll/:serviceName/:columnName``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This is a **very expensive** operation. If you're interested in what goes on under the hood, please refer to the following pseudocode:

.. code:: python

   vertices = vertex list to delete
     for vertex in vertices
         labels = fetch all labels that this vertex is included.
         for label in labels
             for index in label.indices
                 edges = G.read with limit 50K
                 for edge in edges
                     edge.delete



The total complexity is O(L L.I) reads + O(L L.I 50K) writes, worst case. **If the vertex you're trying to delete has more than 50K edges, the deletion will not be consistent**.


Update - POST /mutate/vertex/insert/:serviceName/:columnName
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Basically update on Vertex is same with insert overwrite so use insert for update.

Increment
~~~~~~~~~~~

Not yet implemented; stay tuned.
