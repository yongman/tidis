[![Build Status](https://travis-ci.org/yongman/tidis.svg?branch=master)](https://travis-ci.org/yongman/tidis)
[![Go Report Card](https://goreportcard.com/badge/github.com/yongman/tidis)](https://goreportcard.com/report/github.com/yongman/tidis)
![Project Status](https://img.shields.io/badge/status-alpha-yellow.svg)

# What is Tidis?

Tidis is a Distributed NoSQL database, providing a Redis protocol API (string, list, hash, set, sorted set), written in Go.

Tidis is like [TiDB](https://github.com/pingcap/tidb) layer, providing protocol transform and data structure compute, powered by [TiKV](https://github.com/pingcap/tikv) backend distributed storage which use Raft for data replication and 2PC for distributed transaction.

## Features

* Redis protocol compatible
* Linear scale-out ability
* Storage and computation separation
* Data safety, no data loss, Raft replication
* Transaction support

Any pull requests are welcomed.

## Architecture

*Architechture of tidis*

![architecture](docs/tidis-arch.png)

*Architechture of tikv*

![](https://pingcap.com/images/blog/TiKV_%20Architecture.png)
- Placement Driver (PD): PD is the brain of the TiKV system which manages the metadata about Nodes, Stores, Regions mapping, and makes decisions for data placement and load balancing. PD periodically checks replication constraints to balance load and data automatically.
- Node: A physical node in the cluster. Within each node, there are one or more Stores. Within each Store, there are many Regions.
- Store: There is a RocksDB within each Store and it stores data in local disks.
- Region: Region is the basic unit of Key-Value data movement and corresponds to a data range in a Store. Each Region is replicated to multiple Nodes. These multiple replicas form a Raft group. A replica of a Region is called a Peer.


## 1. Run tidis with docker-compose in one command

```
git clone https://github.com/yongman/tidis-docker-compose.git
cd tidis-docker-compose/
docker-compose up -d
```

Or follow [tidis-docker-compose](https://github.com/yongman/tidis-docker-compose) guide to run with `docker-compose`

## 2. Build or Docker mannually

### Build from source

```
git clone https://github.com/yongman/tidis.git
cd tidis && make
```

### Pull from docker

```
docker pull yongman/tidis
```

### Run TiKV cluster for test

Use `docker run tikv` for test, just follow [PingCAP official guide](https://github.com/pingcap/docs/blob/master/op-guide/docker-deployment.md), you just need to deploy PD and TiKV servers, Tidis will take the role of TiDB.

### Run Tidis or docker

#### Run tidis from executable file

```
bin/tidis-server -conf config.toml
```

#### Run tidis from docker

```
docker run  -d --name tidis -p 5379:5379 -v {your_config_dir}:/data yongman/tidis -conf="/data/config.toml"
```

## 3. Client request

```
redis-cli -p 5379
127.0.0.1:5379> get a
"1"
127.0.0.1:5379> lrange l 0 -1
1) "6"
2) "5"
3) "4"
127.0.0.1:5379> zadd zzz 1 1 2 2 3 3 4 4
(integer) 4
127.0.0.1:5379> zcard zzz
(integer) 4
127.0.0.1:5379> zincrby zzz 10 1
(integer) 11
127.0.0.1:5379> zrange zzz 0 -1 withscores
1) "2"
2) "2"
3) "3"
4) "3"
5) "4"
6) "4"
7) "1"
8) "11"
```


## Already supported commands
### string

    +-----------+-------------------------------------+
    |  command  |               format                |
    +-----------+-------------------------------------+
    |    get    | get key                             |
    +-----------+-------------------------------------+
    |    set    | set key value [EX sec|PX ms][NX|XX] | 
    +-----------+-------------------------------------+
    |   getbit  | getbit key offset                   |
    +-----------+-------------------------------------+
    |   setbit  | setbit key offset value             |
    +-----------+-------------------------------------+
    |    del    | del key1 key2 ...                   |
    +-----------+-------------------------------------+
    |    mget   | mget key1 key2 ...                  |
    +-----------+-------------------------------------+
    |    mset   | mset key1 value1 key2 value2 ...    |
    +-----------+-------------------------------------+
    |    incr   | incr key                            |
    +-----------+-------------------------------------+
    |   incrby  | incr key step                       |
    +-----------+-------------------------------------+
    |    decr   | decr key                            |
    +-----------+-------------------------------------+
    |   decrby  | decrby key step                     |
    +-----------+-------------------------------------+
    |   strlen  | strlen key                          |
    +-----------+-------------------------------------+
    |  pexpire  | pexpire key int                     |
    +-----------+-------------------------------------+
    | pexpireat | pexpireat key timestamp(ms)         |
    +-----------+-------------------------------------+
    |   expire  | expire key int                      |
    +-----------+-------------------------------------+
    |  expireat | expireat key timestamp(s)           |
    +-----------+-------------------------------------+
    |    pttl   | pttl key                            |
    +-----------+-------------------------------------+
    |    ttl    | ttl key                             |
    +-----------+-------------------------------------+

### hash

    +------------+------------------------------------------+
    |  Commands  | Format                                   |
    +------------+------------------------------------------+
    |    hget    | hget key field                           |
    +------------+------------------------------------------+
    |   hstrlen  | hstrlen key                              |
    +------------+------------------------------------------+
    |   hexists  | hexists key                              |
    +------------+------------------------------------------+
    |    hlen    | hlen key                                 |
    +------------+------------------------------------------+
    |    hmget   | hmget key field1 field2 field3...        |
    +------------+------------------------------------------+
    |    hdel    | hdel key field1 field2 field3...         |
    +------------+------------------------------------------+
    |    hset    | hset key field value                     |
    +------------+------------------------------------------+
    |   hsetnx   | hsetnx key field value                   |
    +------------+------------------------------------------+
    |    hmset   | hmset key field1 value1 field2 value2... |
    +------------+------------------------------------------+
    |    hkeys   | hkeys key                                |
    +------------+------------------------------------------+
    |    hvals   | hvals key                                |
    +------------+------------------------------------------+
    |   hgetall  | hgetall key                              |
    +------------+------------------------------------------+
    |   hclear   | hclear key                               |
    +------------+------------------------------------------+
    |  hpexpire  | hpexpire key int                         |
    +------------+------------------------------------------+
    | hpexpireat | hpexpireat key ts                        |
    +------------+------------------------------------------+
    |   hexpire  | hexpire key int                          |
    +------------+------------------------------------------+
    |  hexpireat | hexpireat key ts                         |
    +------------+------------------------------------------+
    |    hpttl   | hpttl key                                |
    +------------+------------------------------------------+
    |    httl    | httl key                                 |
    +------------+------------------------------------------+

### list

    +------------+-----------------------+
    |  commands  |         format        |
    +------------+-----------------------+
    |    lpop    | lpop key              |
    +------------+-----------------------+
    |    rpush   | rpush key             |
    +------------+-----------------------+
    |    rpop    | rpop key              |
    +------------+-----------------------+
    |    llen    | llen key              |
    +------------+-----------------------+
    |   lindex   | lindex key index      |
    +------------+-----------------------+
    |   lrange   | lrange key start stop |
    +------------+-----------------------+
    |    lset    | lset key index value  |
    +------------+-----------------------+
    |    ltrim   | ltrim key start stop  |
    +------------+-----------------------+
    |    ldel    | ldel key              |
    +------------+-----------------------+
    |  lpexipre  | lpexpire key int      |
    +------------+-----------------------+
    | lpexipreat | lpexpireat key ts     |
    +------------+-----------------------+
    |   lexpire  | lexpire key int       |
    +------------+-----------------------+
    |  lexpireat | lexpireat key ts      |
    +------------+-----------------------+
    |    lpttl   | lpttl key             |
    +------------+-----------------------+
    |    lttl    | lttl key              |
    +------------+-----------------------+

### set

    +-------------+--------------------------------+
    |   commands  |             format             |
    +-------------+--------------------------------+
    |     sadd    | sadd key member1 [member2 ...] |
    +-------------+--------------------------------+
    |    scard    | scard key                      |
    +-------------+--------------------------------+
    |  sismember  | sismember key member           |
    +-------------+--------------------------------+
    |   smembers  | smembers key                   |
    +-------------+--------------------------------+
    |     srem    | srem key member                |
    +-------------+--------------------------------+
    |    sdiff    | sdiff key1 key2                |
    +-------------+--------------------------------+
    |    sunion   | sunion key1 key2               |
    +-------------+--------------------------------+
    |    sinter   | sinter key1 key2               |
    +-------------+--------------------------------+
    |  sdiffstore | sdiffstore key1 key2 key3      |
    +-------------+--------------------------------+
    | sunionstore | sunionstore key1 key2 key3     |
    +-------------+--------------------------------+
    | sinterstore | sinterstore key1 key2 key3     |
    +-------------+--------------------------------+
    |    sclear   | sclear key                     |
    +-------------+--------------------------------+
    |   spexpire  | spexpire key int               |
    +-------------+--------------------------------+
    |  spexpireat | spexpireat key ts              |
    +-------------+--------------------------------+
    |   sexpire   | sexpire key int                |
    +-------------+--------------------------------+
    |  sexpireat  | sexpireat key ts               |
    +-------------+--------------------------------+
    |    spttl    | spttl key                      |
    +-------------+--------------------------------+
    |     sttl    | sttl key                       |
    +-------------+--------------------------------+

### sorted set

    +------------------+---------------------------------------------------------------+
    |     commands     |                             format                            |
    +------------------+---------------------------------------------------------------+
    |       zadd       | zadd key member1 score1 [member2 score2 ...]                  |
    +------------------+---------------------------------------------------------------+
    |       zcard      | zcard key                                                     |
    +------------------+---------------------------------------------------------------+
    |      zrange      | zrange key start stop [WITHSCORES]                            |
    +------------------+---------------------------------------------------------------+
    |     zrevrange    | zrevrange key start stop [WITHSCORES]                         |
    +------------------+---------------------------------------------------------------+
    |   zrangebyscore  | zrangebyscore key min max [WITHSCORES][LIMIT offset count]    |
    +------------------+---------------------------------------------------------------+
    | zrevrangebyscore | zrevrangebyscore key max min [WITHSCORES][LIMIT offset count] |
    +------------------+---------------------------------------------------------------+
    | zremrangebyscore | zremrangebyscore key min max                                  |
    +------------------+---------------------------------------------------------------+
    |    zrangebylex   | zrangebylex key min max [LIMIT offset count]                  |
    +------------------+---------------------------------------------------------------+
    |  zrevrangebylex  | zrevrangebylex key max min [LIMIT offset count]               |
    +------------------+---------------------------------------------------------------+
    |  zremrangebylex  | zremrangebylex key min max                                    |
    +------------------+---------------------------------------------------------------+
    |      zcount      | zcount key                                                    |
    +------------------+---------------------------------------------------------------+
    |     zlexcount    | zlexcount key                                                 |
    +------------------+---------------------------------------------------------------+
    |      zscore      | zscore key member                                             |
    +------------------+---------------------------------------------------------------+
    |       zrem       | zrem key member1 [member2 ...]                                |
    +------------------+---------------------------------------------------------------+
    |      zclear      | zclear key                                                    |
    +------------------+---------------------------------------------------------------+
    |      zincrby     | zincrby key increment member                                  |
    +------------------+---------------------------------------------------------------+
    |     zpexpire     | zpexpire key int                                              |
    +------------------+---------------------------------------------------------------+
    |    zpexpireat    | zpexpireat key ts                                             |
    +------------------+---------------------------------------------------------------+
    |      zexpire     | zexpire key int                                               |
    +------------------+---------------------------------------------------------------+
    |     zexpireat    | zexpireat key ts                                              |
    +------------------+---------------------------------------------------------------+
    |       zpttl      | zpttl key                                                     |
    +------------------+---------------------------------------------------------------+
    |       zttl       | zttl key                                                      |
    +------------------+---------------------------------------------------------------+

### Transaction

    +---------+---------+
    | command | support |
    +---------+---------+
    |  multi  | Yes     |
    +---------+---------+
    |   exec  | Yes     |
    +---------+---------+

## Benchmark

[base benchmark](https://github.com/yongman/tidis/wiki/Tidis-base-benchmark)

## License

Tidis is under the MIT license. See the [LICENSE](./LICENSE) file for details.

## Acknowledgment

* Thanks [PingCAP](https://github.com/pingcap) for providing [tikv](https://github.com/pingcap/tikv) and [pd](https://github.com/pingcap/pd) powerful components.
* Thanks [RocksDB](https://github.com/facebook/rocksdb) for their powerful storage engines.
