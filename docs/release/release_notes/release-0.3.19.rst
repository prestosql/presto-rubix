==============
Release 0.3.19
==============

Fixes and Features
------------------
* Add an implementation of PrestoClusterManager that does not cache the list of worker nodes. Set `rubix.cluster.manager.presto.class` as `io.prestosql.rubix.prestosql.SyncPrestoClusterManager` to use the new implementation.