Getting Started
===============

********************
The Directory Layout
********************

Once extracted the downloaded binary release of S2Graph or built from the source as described below, the following files and directories should be found in the directory.

.. code::

  DISCLAIMER
  LICENCE               # the Apache License 2.0
  NOTICE
  bin                   # scripts to manage the lifecycle of S2Graph
  conf                  # configuration files
  lib                   # contains the binary
  logs                  # application logs
  var                   # application data

*****************
Launching S2Graph
*****************

The following will launch S2Graph, using HBase in the standalone mode for data storage and H2 as the metadata storage.

.. code::

  sh bin/start-s2graph.sh

To connect to a remote HBase cluster or use MySQL as the metastore, refer to the instructions in ``conf/application.conf``

************************
Building from the Source
************************

We use SBT to build the project, which can be installed using Homebrew on MacOS (brew install sbt). For other operating systems, refer to the SBT Tutorial. Once SBT is installed, running the following command on the source root will build the project from the source:

.. code:: bash

  sbt package

Depending on the internet connection, the initial run might take a while downloading the required dependencies. Once the build is complete, the same directory layout as in the top of this document can be found at ``target/apache-s2graph-$version-bin``, where ``$version`` is the current version of the project, e.g. ``0.1.0-incubating``
