****************
Query Vertices
****************


POST - ``/graphs/getVertices``
--------------------------------

Selecting all vertices from serviceColumn account_id of a service s2graph.

.. code:: bash

    curl -XPOST localhost:9000/graphs/getVertices -H 'Content-Type: Application/json' -d '
    [
        {"serviceName": "s2graph", "columnName": "account_id", "ids": [1, 2, 3]},
        {"serviceName": "agit", "columnName": "user_id", "ids": [1, 2, 3]}
    ]'
