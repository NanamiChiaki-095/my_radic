# MyRadic: 基于 Golang 的高性能分布式实时搜索引擎

[![Go Version](https://img.shields.io/badge/Go-1.25+-00ADD8?style=flat&logo=go)](https://golang.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Architecture](https://img.shields.io/badge/Arch-Distributed-orange.svg)]()

`MyRadic` 是一个从零手写的工业级分布式搜索引擎。项目不仅实现了高性能的倒排索引内核，更在架构层面实践了**微服务拆分、存算分离、分布式一致性、防击穿保护**等高级模式。

---

## 🚀 核心亮点 (Key Highlights)

### 1. 硬核底层索引内核
*   **手写倒排索引**：采用 **跳表 (SkipList)** 实现 Posting List。相比数组，跳表支持 **O(log N)** 的快速查找与 `SkipTo` 操作，极大地加速了多路归并（AND/OR）查询。
*   **存算分离存储**：
    *   **计算层 (In-Memory)**：内存跳表承载实时检索与 **TF-IDF** 打分计算。
    *   **存储层 (Disk-Based)**：集成 **BoltDB (B+ Tree)** 作为本地 WAL 与正排索引存储，支持**秒级崩溃恢复**，无需依赖重型外部数据库重建索引。

### 2. 金融级数据一致性方案
*   **Transactional Outbox 模式**：通过本地事务原子性，将“业务数据写库”与“同步指令发件”绑定，彻底解决了微服务架构下的**双写一致性**难题。
*   **可靠异步同步**：基于 **Kafka** 实现削峰填谷，结合后台 **Relay Service** 保证了消息的 **At-Least-Once** 投递。
*   **全链路幂等**：Index Service 端的写入逻辑采用覆盖更新设计，天然抵抗消息重发干扰。

### 3. 高并发架构优化
*   **防击穿机制**：API Gateway 集成 **SingleFlight** 归并回源，将海量并发的热点搜索合并为单次 RPC 调用，保护后端索引节点。
*   **多级缓存策略**：实现 **Redis (Cache Aside) + 内存二级缓存**，热点请求响应延迟控制在微秒级。
*   **Scatter-Gather 检索**：搜索请求通过 gRPC 并发广播至各分片节点，实现分布式并行计算。

### 4. 工业级工程实践
*   **微服务治理**：基于 **gRPC + Etcd** 实现服务注册发现、心跳检测与客户端负载均衡。
*   **全方位可观测性**：集成 **Prometheus** 监控，实时暴露 QPS、Latency、Goroutine 数量等核心指标。
*   **云原生部署**：提供完整的 Docker 与 **K8s** 部署配置，支持集群一键拉起。

---

## 📊 性能表现 (Benchmark)

在单机开发环境 (8-Core CPU, 16GB RAM, Windows) 下进行的压力测试结果：

| 测试场景 | QPS/TPS | 说明 |
| :--- | :--- | :--- |
| **文档写入** | **1,559 TPS** | 受限于单机 MySQL 磁盘事务 IO (开启 Transactional Outbox 模式) |
| **热点搜索** | **11,099 QPS** | 100% 缓存命中，由 Redis + json-iterator 提供极速响应 |
| **混合搜索** | **4,067 QPS** | 在 1500 TPS 持续写入压力下的并发搜索，验证了读写隔离能力 |
| **冷启动搜索** | **2,733 QPS** | 100% 缓存穿透 (随机词)，验证了核心 SkipList 倒排索引的抗压能力 |

> **注**: 搜索接口 P99 延迟在热点场景下 **< 2ms**，在全冷启动场景下稳定在 **37ms** 左右。

---

## 🛠️ 技术栈 (Tech Stack)

| 领域 | 技术选型 |
| :--- | :--- |
| **语言/框架** | Golang, Gin, gRPC |
| **索引内核** | SkipList, BoltDB, Jieba 分词 |
| **基础设施** | MySQL, Redis, Kafka, Etcd |
| **可观测性** | Prometheus |
| **容器化** | Docker, Kubernetes |

---

## 📂 目录导航

*   [系统架构详述](./ARCHITECTURE.md) —— 深度解析设计理念与数据流。
*   [分布式同步指南](./INTERVIEW_SYNC_GUIDE.md) —— 应对面试中关于数据一致性的深挖。
*   [API 接口文档](./api_gateway/README.md) —— 快速上手指南。

---

## 🚦 快速开始

```bash
# 1. 启动基础设施 (MySQL, Redis, Kafka, Etcd)
docker-compose up -d

# 2. 启动 Index Service
go run main.go

# 3. 启动 API Gateway
go run api_gateway/main.go -port 8080

# 4. 执行压力测试
go run util/loader/main.go
```
