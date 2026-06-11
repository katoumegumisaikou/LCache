# 本地缓存与 Redis 操作耗时对比

测试时间：2026-06-11  
测试机器：AMD Ryzen 7 5800H with Radeon Graphics，linux/amd64  
Go 版本：go1.26.1 linux/amd64  
Redis 版本：Redis server v=7.4.9，Docker 容器 `lcache-redis-bench`

## 测试范围

本次只对比单机本地缓存和本机 Redis 的基础操作耗时：

- 本地缓存：`Cache.Set`、`Cache.Get` 命中、`Cache.Get` 未命中、`Cache.Delete` 命中、`Group.Get` 本地命中
- Redis：单连接、无 pipeline 的 `SET`、`GET`、`DEL`

本地缓存 benchmark 不涉及 gRPC、etcd、ClientPicker、网络调用。

## 测试命令

本地缓存：

```bash
go test -run '^$' -bench '^BenchmarkLocal' -benchmem -benchtime=3s
```

Redis SET/GET：

```bash
docker exec lcache-redis-bench redis-cli flushall
docker exec lcache-redis-bench redis-benchmark \
  -h 127.0.0.1 -p 6379 \
  -c 1 -n 100000 -P 1 \
  -d 15 -r 4096 \
  --precision 3 \
  -t set,get
```

Redis 多连接 SET/GET：

```bash
docker exec lcache-redis-bench redis-cli flushall
docker exec lcache-redis-bench redis-benchmark \
  -h 127.0.0.1 -p 6379 \
  -c 50 -n 100000 -P 1 \
  -d 15 -r 4096 \
  --precision 3 \
  -t set,get
```

Redis pipeline SET/GET：

```bash
docker exec lcache-redis-bench redis-cli flushall
docker exec lcache-redis-bench redis-benchmark \
  -h 127.0.0.1 -p 6379 \
  -c 1 -n 100000 -P 16 \
  -d 15 -r 4096 \
  --precision 3 \
  -t set,get
```

宿主机原生 Redis SET/GET：

```bash
redis-server --port 6381 --bind 127.0.0.1 --save "" --appendonly no --daemonize yes
redis-cli -p 6381 flushall
redis-benchmark -h 127.0.0.1 -p 6381 -c 1 -n 100000 -P 1 -d 15 -r 4096 --precision 3 -t set,get
redis-benchmark -h 127.0.0.1 -p 6381 -c 50 -n 100000 -P 1 -d 15 -r 4096 --precision 3 -t set,get
redis-benchmark -h 127.0.0.1 -p 6381 -c 1 -n 100000 -P 16 -d 15 -r 4096 --precision 3 -t set,get
redis-cli -p 6381 shutdown
```

Redis DEL：

```bash
docker exec lcache-redis-bench redis-cli flushall
docker exec lcache-redis-bench redis-benchmark \
  -h 127.0.0.1 -p 6379 \
  -c 1 -n 100000 -P 1 \
  -d 15 -r 100000 \
  --precision 3 \
  -t set -q

docker exec lcache-redis-bench redis-benchmark \
  -h 127.0.0.1 -p 6379 \
  -c 1 -n 100000 -P 1 \
  -r 100000 \
  --precision 3 \
  DEL key:__rand_int__
```

## 本地缓存结果

| Benchmark | 耗时 | 内存分配 |
|---|---:|---:|
| `BenchmarkLocalCache_Set` | 124.5 ns/op | 56 B/op, 2 allocs/op |
| `BenchmarkLocalCache_GetHit` | 76.87 ns/op | 0 B/op, 0 allocs/op |
| `BenchmarkLocalCache_GetMiss` | 25.22 ns/op | 0 B/op, 0 allocs/op |
| `BenchmarkLocalCache_SetThenGet` | 144.7 ns/op | 56 B/op, 2 allocs/op |
| `BenchmarkLocalCache_DeleteHit` | 76.98 ns/op | 0 B/op, 0 allocs/op |
| `BenchmarkLocalCache_ParallelGetHit` | 42.74 ns/op | 0 B/op, 0 allocs/op |
| `BenchmarkLocalGroup_GetHit` | 73.89 ns/op | 0 B/op, 0 allocs/op |

## Redis 结果

### 单连接，无 pipeline

| Redis 操作 | 吞吐 | 平均延迟 | p50 | p95 | p99 | max |
|---|---:|---:|---:|---:|---:|---:|
| `SET` | 8469.55 req/s | 0.106 ms | 0.103 ms | 0.135 ms | 0.215 ms | 1.143 ms |
| `GET` | 7777.26 req/s | 0.117 ms | 0.103 ms | 0.135 ms | 0.183 ms | 1174.527 ms |
| `DEL` | 7589.56 req/s | 0.120 ms | 0.103 ms | 0.135 ms | 0.231 ms | 1398.783 ms |

### 多连接，无 pipeline

参数：`-c 50 -P 1`

| Redis 操作 | 吞吐 | 平均延迟 | p50 | p95 | p99 | max |
|---|---:|---:|---:|---:|---:|---:|
| `SET` | 53475.94 req/s | 0.487 ms | 0.463 ms | 0.687 ms | 0.943 ms | 2.239 ms |
| `GET` | 55005.50 req/s | 0.471 ms | 0.455 ms | 0.631 ms | 0.879 ms | 1.767 ms |

### 单连接，pipeline

参数：`-c 1 -P 16`

| Redis 操作 | 吞吐 | 平均延迟 | p50 | p95 | p99 | max |
|---|---:|---:|---:|---:|---:|---:|
| `SET` | 52110.47 req/s | 0.293 ms | 0.119 ms | 0.159 ms | 0.247 ms | 1063.935 ms |
| `GET` | 122249.38 req/s | 0.117 ms | 0.119 ms | 0.151 ms | 0.239 ms | 0.463 ms |

### 宿主机原生 Redis

版本：Redis server v=7.0.15  
参数同上，监听 `127.0.0.1:6381`，关闭 RDB 和 AOF 持久化。

| 模式 | Redis 操作 | 吞吐 | 平均延迟 | p50 | p95 | p99 | max |
|---|---|---:|---:|---:|---:|---:|---:|
| 单连接，无 pipeline | `SET` | 8285.00 req/s | 0.109 ms | 0.111 ms | 0.143 ms | 0.231 ms | 1.055 ms |
| 单连接，无 pipeline | `GET` | 8218.95 req/s | 0.110 ms | 0.111 ms | 0.135 ms | 0.183 ms | 1.087 ms |
| 多连接，无 pipeline | `SET` | 51361.07 req/s | 0.508 ms | 0.487 ms | 0.711 ms | 0.967 ms | 1.535 ms |
| 多连接，无 pipeline | `GET` | 52742.62 req/s | 0.492 ms | 0.471 ms | 0.663 ms | 0.903 ms | 1.799 ms |
| 单连接，pipeline=16 | `SET` | 114810.56 req/s | 0.126 ms | 0.127 ms | 0.159 ms | 0.255 ms | 0.431 ms |
| 单连接，pipeline=16 | `GET` | 117508.81 req/s | 0.123 ms | 0.119 ms | 0.159 ms | 0.247 ms | 0.975 ms |

## 换算对比

为方便直观看差距，将 Redis p50 延迟换算为纳秒：

- Redis `0.103 ms` = `103000 ns`
- Redis `0.106 ms` = `106000 ns`
- Redis `0.117 ms` = `117000 ns`
- Redis `0.120 ms` = `120000 ns`

| 操作 | 本地缓存 | Redis p50 | Redis 平均 | Redis p50 / 本地 |
|---|---:|---:|---:|---:|
| 写入 `SET` | 124.5 ns/op | 103000 ns | 106000 ns | 约 827 倍 |
| 读取命中 `GET` | 76.87 ns/op | 103000 ns | 117000 ns | 约 1340 倍 |
| 删除命中 `DEL` | 76.98 ns/op | 103000 ns | 120000 ns | 约 1338 倍 |

## 结论

在本机单进程内存访问路径上，本地缓存读命中和删除命中约为几十纳秒级，写入约为百纳秒级。Redis 即使运行在同一台机器的 Docker 容器内，并且使用单连接、无 pipeline，基础命令 p50 仍约为 0.103 ms，也就是约 10 万纳秒级。

这说明：

- 本地缓存适合极低延迟热数据读取。
- Redis 适合跨进程共享、集中存储、过期策略、持久化、分布式访问等场景。
- 二者不是同一层级的替代关系，本地缓存更像进程内加速层，Redis 更像外部缓存服务。

## 补测结论：多连接与 pipeline

Redis 多连接和 pipeline 都能明显提高吞吐：

| 模式 | SET 吞吐 | GET 吞吐 |
|---|---:|---:|
| 单连接，无 pipeline | 8469.55 req/s | 7777.26 req/s |
| 多连接，无 pipeline | 53475.94 req/s | 55005.50 req/s |
| 单连接，pipeline=16 | 52110.47 req/s | 122249.38 req/s |
| 宿主机单连接，无 pipeline | 8285.00 req/s | 8218.95 req/s |
| 宿主机多连接，无 pipeline | 51361.07 req/s | 52742.62 req/s |
| 宿主机单连接，pipeline=16 | 114810.56 req/s | 117508.81 req/s |

但它们优化的是吞吐，不是把 Redis 变成本地函数调用：

- 多连接把更多请求同时压到 Redis 上，吞吐提升，但单个请求在服务端/网络栈里会排队，所以 p50 延迟从约 `0.103 ms` 提升到约 `0.46 ms`。
- pipeline 把多个请求合并在一次 TCP 往返里，显著摊薄网络往返成本，尤其 GET 吞吐提升到 `122249.38 req/s`。
- pipeline 的延迟统计是按批量发送后的请求完成时间统计，吞吐参考价值更高；不能直接等同于“每个业务请求独立往返”的延迟。
- 即便 pipeline 后 Redis p50 仍约 `0.119 ms`，也就是约 `119000 ns`，仍远高于本地缓存几十纳秒的热路径。
- 宿主机原生 Redis 去掉了 Docker 端口映射，但单连接 p50 仍约 `0.111 ms`，和 Docker 容器的 `0.103 ms` 同量级；这说明主要成本不是 Docker，而是外部 Redis 服务调用的网络/协议/进程边界。
- 宿主机 pipeline SET 本次吞吐高于 Docker pipeline SET，达到 `114810.56 req/s`，但 p50 仍是 `0.127 ms`，仍不是本地缓存纳秒级。

## 注意事项

- 本地 `Set` 和 `SetThenGet` 使用 LRU 存储测试，因为当前 LRU2 对已有 key 的 `put` 返回 `false`，上层会把它当作 Set 失败；这会干扰写入 benchmark。
- 本地 `GetHit`、`GetMiss`、`ParallelGetHit`、`Group.GetHit` 使用 LRU2 默认配置。
- Redis `DEL` 使用随机 key，预填后删除过程中会出现重复 key，因此结果包含部分删除不存在 key 的情况；这对网络往返延迟影响较小，但不是严格的“每次都删除存在 key”。
- Redis `GET` 和 `DEL` 本次各出现一个 1 秒级 max 离群点，p50/p95/p99 更能代表稳定路径。
- pipeline SET 本次也出现一个 1 秒级 max 离群点，p50/p95/p99 更适合作为稳定路径参考。
- Redis 测的是外部服务调用，包含 TCP、本机 Docker 网络栈、Redis 命令处理和响应解析；本地缓存测的是进程内函数调用。

停止 Redis benchmark 容器：

```bash
docker stop lcache-redis-bench
docker rm lcache-redis-bench
```
