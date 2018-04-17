# Tidis

Tidis is a Distributed NoSQL database, providing a redis-protocal api(string,list,hash,set,sorted-set), written in Go.

Tidis is like [TiDB](https://github.com/pingcap/tidb) layer, providing protocal transform, powered by [tikv](https://github.com/pingcap/tikv) backend distributed storage which use raft for data replication and 2PC for distributed trasaction.

This repo is WIP now and has lots of work to do, and for test only.
