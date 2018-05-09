# Tidis

Tidis is a Distributed NoSQL database, providing a redis-protocol api(string,list,hash,set,sorted-set), written in Go.

Tidis is like [TiDB](https://github.com/pingcap/tidb) layer, providing protocol transform, powered by [tikv](https://github.com/pingcap/tikv) backend distributed storage which use raft for data replication and 2PC for distributed transaction.

This repo is `WIP` now and has lots of work to do, and for test only.

Any pull requests are welcomed.
## Architecture

![architecture](docs/tidis-arch.png)

## Build

```
git clone https://github.com/yongman/tidis.git
make
```

## Run tikv cluster for test

Use docker run tikv for test, just follow [PingCAP official guide](https://github.com/pingcap/docs/blob/master/op-guide/docker-deployment.md), you just need to deploy pd and tikv servers, Tidis will take the role of Tidb.

## Run tidis

```
bin/bin/tidis-server -backend <pd address, ip:port>
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


## Already supported Commands
### string
| string |
|--------|
| get    |
| set    |
| del    |
| mget   |
| mset   |
| incr   |
| incrby |
| decr   |
| decrby |
| strlen |

### hash
| hash    |
|---------|
| hget    |
| hstrlen |
| hexists |
| hlen    |
| hmget   |
| hdel    |
| hset    |
| hsetnx  |
| hmset   |
| hkeys   |
| hvals   |
| hgetall |
| hclear  |

### list
| list   |
|--------|
| lpop   |
| rpush  |
| rpop   |
| llen   |
| lindex |
| lrange |
| lset   |
| ltrim  |
| ldel   |

### set
| set         |
|-------------|
| sadd        |
| scard       |
| sismember   |
| smembers    |
| srem        |
| sdiff       |
| sunion      |
| sinter      |
| sdiffstore  |
| sunionstore |
| sinterstore |
| sclear      |

### sorted set
| sorted set       |
|------------------|
| zadd             |
| zcard            |
| zrange           |
| zrevrange        |
| zrangebyscore    |
| zrevrangebyscore |
| zremrangebyscore |
| zrangebylex      |
| zrevrangebylex   |
| zremrangebylex   |
| zcount           |
| zlexcount        |
| zscore           |
| zrem             |
| zclear           |
| zincrby          |
