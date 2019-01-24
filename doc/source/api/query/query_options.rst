****************
Query Options
****************

Query Level Options
-----------------------

A typical query contains the source vertex as a starting point, a list of labels to traverse, and optional filters or weights for unwanted results and sorting respectively. Query requests are structured as follows


QUERY LEVEL TABLE HERE

**Step**

Each step tells S2Graph which labels to traverse in the graph and how to traverse them. Labels in the very first step should be directly connected to the source vertices, labels in the second step should be directly connected to the result vertices of the first step traversal, and so on. A step is specified with a list of ``query params`` which we will cover in detail below.


Step Level Option
-------------------------

STEP LEVEL TABLE HERE


Query Param Level Option
-----------------------------

Query PARAM LEVEL TABLE HERE
