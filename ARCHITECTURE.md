# MyRadic - 分布式实时搜索引擎

## 1. 项目简介

`my_radic` 是一个基于 Go 语言实现的**工业级分布式搜索引擎**。项目定位于解决中大规模文本检索场景下的**高并发**、**低延迟**与**强一致性**问题。

本项目不依赖 ElasticSearch 等现成组件，而是**从零手写**了倒排索引内核、分词器集成、微服务通信框架及分布式事务保障机制。

---

## 2. 核心架构设计

### 2.1 架构全景图 (Microservices Architecture)

```mermaid
graph TD
    User[用户/浏览器] -->|WebSocket/HTTP| Gateway[API Gateway (BFF层)]
    
    subgraph "接入层 (High Availability)"
        Gateway -->|1. 读写分离| Redis[Redis Cache]
        Gateway -->|2. 事务双写| MySQL[(MySQL Source)]
    end
    
    subgraph "异步消息层 (Decoupling)"
        MySQL -->|3. Relay 轮询| Kafka[Kafka Cluster]
    end
    
    subgraph "索引核心层 (Sharding)"
        Kafka -->|4. 批量消费| IndexNode1[Index Service 节点1]
        Kafka -->|4. 批量消费| IndexNode2[Index Service 节点2]
        
        IndexNode1 -->|gRPC| Gateway
        IndexNode2 -->|gRPC| Gateway
    end
```

### 2.2 关键组件

| 组件 | 技术选型 | 核心职责 |
| :--- | :--- | :--- |
| **API Gateway** | Gin + Gorilla WebSocket | 流量入口，负责协议转换、鉴权、**SingleFlight 防击穿**、**Result Cache**。 |
| **Index Service** | gRPC + Etcd | 索引构建与检索服务。无状态设计，支持 K8s 水平扩容。 |
| **KV Storage** | BoltDB / BadgerDB | **正排索引存储**。作为 WAL (Write-Ahead Log) 保证数据持久化。 |
| **In-Memory Index** | SkipList (跳表) | **倒排索引**。基于跳表实现高效的 `Posting List` 存储与交集运算。 |
| **Message Queue** | Kafka | **削峰填谷**。解耦入库与索引构建，支持批量写入优化 IO。 |

---

## 3. 核心难点与解决方案 (面试重点)

### 难点一：分布式环境下的数据最终一致性
**挑战**：用户发布文章写入 MySQL 后，如果直接调 RPC 通知索引服务，可能会因为网络抖动导致“文章已发布但搜不到”；反之，如果先发消息后写库，可能导致“幽灵消息”。

**解决方案：Transactional Outbox 模式 (事务发件箱)**
1.  **原理**：利用 MySQL 本地事务的原子性。在写入 `articles` 表的同一个事务中，将消息写入 `outbox` 表。
2.  **流程**：
    *   `Begin Tx` -> `Insert Article` -> `Insert Outbox` -> `Commit Tx`.
    *   **Relay Service**（后台协程）轮询 `outbox` 表，将状态为 `Pending` 的消息投递到 Kafka。
    *   投递成功后更新状态为 `Sent`。
    *   **收益**：确保了**业务落库**与**消息发送**的强一致性，实现了 At-Least-Once（至少一次）投递，彻底解决了数据丢失问题。

#### 代码实现细节 (Code Implementation)

1.  **数据库设计 (MySQL Schema)**
    *   `articles` 表：存储业务数据。
    *   `outbox` 表：存储待发送的消息。关键字段包括 `topic` (Kafka主题), `payload` (二进制数据), `status` (0:Pending, 1:Sent)。
    ```sql
    CREATE TABLE `outbox` (
      `id` bigint unsigned AUTO_INCREMENT,
      `topic` varchar(255) NOT NULL,
      `payload` longblob,
      `status` tinyint DEFAULT 0, -- 0:待发送, 1:已发送
      PRIMARY KEY (`id`)
    );
    ```

2.  **双写流程 (API Gateway)**
    *   代码位置：`api_gateway/main.go`
    *   利用 GORM 的 `tx := db.Begin()` 开启事务。
    *   先后执行 `tx.Create(&article)` 和 `tx.Create(&outboxMsg)`。
    *   最后 `tx.Commit()`。

3.  **异步中继 (Relay Service)**
    *   代码位置：`api_gateway/infra/relay.go`
    *   **轮询机制**：后台 Goroutine 每隔 50ms 轮询一次 DB。
    *   **批量处理**：每次 `Limit(100)` 取出 `status=0` 的消息。
    *   **发送与确认**：调用 Sarama SDK 发送 Kafka，成功后回调更新 MySQL `status=1`。


### 难点二：高并发下的搜索性能与稳定性
**挑战**：热门关键词（如 "iPhone"）可能会引发瞬间高并发流量（热点击穿），且倒排链过长会导致计算耗时。

**解决方案：多级缓存 + 防击穿机制**
1.  **SingleFlight (归并回源)**：
    *   在 API Gateway 层引入 `golang.org/x/sync/singleflight`。
    *   **效果**：当 1000 个用户同时搜索 "Golang" 时，系统**只向后端发起 1 次 RPC 调用**，所有请求共享这一个结果。极大降低了下游负载。
2.  **Cache Aside Pattern**：
    *   优先查 Redis，未命中查 RPC 并回写。设置较短 TTL (如 1分钟) 保证热点数据的实时性。

### 难点三：复杂布尔查询的高效执行
**挑战**：如何高效支持 `(A AND B) OR (C AND D)` 这样复杂的嵌套逻辑？普通的数组求交集复杂度为 O(N)，在大数据量下性能极差。

**解决方案：基于跳表 (SkipList) 的多路归并**
1.  **数据结构**：倒排链（Posting List）使用**跳表**而非数组存储 DocID。
2.  **算法优化**：
    *   利用跳表的 `SkipTo(target)` 能力，在求交集（AND）时，可以直接**跳过**大量不匹配的 ID。
    *   复杂度从线性的 O(N) 降低到 O(M * log N)，其中 M 为较短链的长度。
3.  **位图过滤**：在跳表节点中嵌入 `Bitmap`，在遍历时直接进行属性过滤（如“只看有图的文章”），无需回查正排。

### 难点四：索引服务的快速恢复与持久化
**挑战**：倒排索引全在内存中，服务重启会导致数据丢失；如果每次重启都全量拉取 MySQL，启动时间过长且数据库压力大。

**解决方案：WAL (Write-Ahead Log) 思想**
1.  **写入流程**：文档先序列化写入本地嵌入式数据库 (**BoltDB**)，作为“正排索引”和持久化日志。
2.  **内存构建**：写入 BoltDB 成功后，更新内存中的 SkipList。
3.  **重启恢复**：服务启动时，直接扫描本地 BoltDB 文件重建内存倒排索引。
4.  **收益**：利用顺序读写的高效性，实现了秒级启动恢复，且无需依赖外部 MySQL。

---

## 4. 目录结构说明

```bash
├── api_gateway/      # [BFF层] 处理 HTTP/WebSocket 请求
│   ├── infra/        # 基础设施 (Redis, MySQL, Kafka, Relay)
│   └── rpc/          # gRPC 客户端封装 (含 LoadBalancer)
├── index_service/    # [核心服务] 索引构建与检索
│   ├── consumer.go   # Kafka 消费者 (批量处理)
│   └── service_hub.go # Etcd 服务注册
├── internal/
│   ├── indexer/      # 索引器门面 (协调正排与倒排)
│   ├── kvdb/         # 正排索引存储 (BoltDB 实现)
│   └── reverse_index/# 倒排索引核心 (SkipList 实现)
├── types/            # Protobuf 定义与生成的代码
└── deploy/           # K8s 部署配置 (YAML)
```

## 5. 快速启动

### 依赖环境
*   Docker & Kubernetes (Optional)
*   Etcd, Kafka, Redis, MySQL

### 编译与运行
```bash
# 1. 启动依赖设施
docker-compose up -d

# 2. 启动索引服务 (支持多节点)
go run index_service/main.go -port 50051

# 3. 启动网关
go run api_gateway/main.go -port 8080

# 4. 访问测试
open http://localhost:8080
```

---

## 5. 核心技术原理深析 (Deep Dive & Interview FAQ)

### 5.1 SingleFlight 原理与源码剖析
**原理**：SingleFlight 的核心作用是**请求合并**。它通过一个全局的 `Map` 记录正在进行的请求。
*   **结构**：`Group` 结构体包含 `mu sync.Mutex` 和 `m map[string]*call`。`call` 结构体包含 `wg sync.WaitGroup` 和 `val, err`。
*   **流程**：
    1.  **Lock**: 获取互斥锁。
    2.  **Check Map**: 用 Key 去 Map 查，看是否已有相同请求在处理 (`call` 存在)。
    3.  **If Exists (命中)**: 释放锁，调用 `c.wg.Wait()` 阻塞等待，最后直接返回结果。
    4.  **If Not Exists (未命中)**: 创建新 `call`，存入 Map，`wg.Add(1)`，释放锁。执行真正的函数 `fn()`。
    5.  **Finish**: `fn()` 返回后，加锁，从 Map 删掉 Key，`wg.Done()` 唤醒所有等待者。

**FAQ (面试官追问)**:
*   **Q: 如果那个唯一的请求超时了或 Panic 了怎么办？**
    *   A: `singleflight` 处理了 Panic，会 recover 并抛出。如果超时，所有等待的请求都会收到超时错误（通常需要在 `fn` 内部处理 context 超时）。
*   **Q: 为什么不用互斥锁直接锁住整个函数？**
    *   A: 互斥锁是**串行化**（一个接一个），SingleFlight 是**并行合并**（一批只执行一次），吞吐量天差地别。

### 5.2 倒排索引：为什么选择跳表 (SkipList)?
**原理**：跳表是一种**概率性平衡**的数据结构，通过维护多层链表来加速查找。底层是完整链表，上层是稀疏索引。
*   **Search**: 从最高层开始，`Key > Next` 则右移，`Key < Next` 则下潜。复杂度 O(log N)。
*   **Intersection (求交集)**:
    *   假设有倒排链 A: `[1, 5, 9, ...]` 和 B: `[2, 5, 8, ...]`.
    *   指针 `pA=1`, `pB=2`. `pA < pB`，利用跳表特性，A 可以直接 **SkipTo(2)**（甚至更大），而不需要一步步 `Next()`。这是数组无法比拟的优势。

**FAQ (面试官追问)**:
*   **Q: 为什么不用红黑树 (Red-Black Tree)?**
    *   A: 1. **范围查询**: 倒排索引常需要区间扫描，红黑树需要回溯，跳表直接遍历底层链表即可。2. **实现难度**: 红黑树旋转极其复杂，跳表代码简单。3. **并发友好**: 跳表只需局部 CAS 锁即可实现无锁并发，红黑树调整树高涉及全局锁。
*   **Q: 为什么不用 B+ 树？**
    *   A: B+ 树更适合磁盘存储（减少 IO 次数），纯内存场景下，跳表的指针跳转比 B+ 树的节点分裂/合并开销更小。

### 5.3 分布式事务：Transactional Outbox 深度思考
**原理**：将“发送消息”转化为“数据库写操作”。
*   **原子性**: MySQL 保证 `Insert Business` 和 `Insert Message` 要么全成功，要么全失败。
*   **At-Least-Once**: Relay 可能会 crash。如果 Relay 发送了 Kafka 但没来得及 Update DB，重启后会重发。
    *   **应对**: 消费者端 (Index Service) 必须实现**幂等性 (Idempotency)**。我的设计中，倒排索引的 `Add` 操作是幂等的（多次 Add 相同 DocID 只会覆盖或忽略）。

**FAQ (面试官追问)**:
*   **Q: 为什么不先发 Kafka 再 Commit DB?**
    *   A: 发 Kafka 无法回滚。如果发成功了但 DB Commit 失败（死锁/断电），Kafka 消费者会读到一条“脏数据”（业务里实际上没有这篇文档）。
*   **Q: 为什么不先 Commit DB 再发 Kafka?**
    *   A: Commit 返回后进程立马挂了，Kafka 消息就丢了。导致数据不一致。
*   **Q: Relay 轮询数据库会不会有性能瓶颈？**
    *   A: 会。生产环境可以用 **CDC (Change Data Capture)** 如 Debezium 监听 Binlog 来代替轮询，降低 DB 压力。

### 5.4 存储引擎：BoltDB vs LSM Tree
**原理**：
*   **BoltDB (B+ Tree)**: 读多写少场景。使用 `mmap` 将文件映射到内存，利用 OS 的 Page Cache 管理缓存。支持 **COW (Copy-On-Write)**，读事务无锁，写事务串行。
*   **BadgerDB (LSM Tree)**: 写多读少场景。写入只追加 MemTable (内存) 和 WAL (磁盘)，定期 Flush 到 SSTable。

**FAQ (面试官追问)**:
*   **Q: 为什么正排索引选 BoltDB？**
    *   A: 文档详情查询通常是随机读，B+ 树读性能稳定。而且文档一旦写入修改较少，符合读多写少特性。
*   **Q: 如果换成写操作极高的场景（如日志索引），你会怎么改？**
    *   A: 我会把底层 KV 换成 **BadgerDB** 或 **RocksDB**。LSM Tree 将随机写转化为顺序写，吞吐量高 1-2 个数量级。我的代码通过 `KVStore` 接口隔离了实现，切换引擎只需改一行代码。
```
