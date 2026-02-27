# 分布式压测 Runbook（k6 + Kubernetes）

本文档给出 `my_radic` 在 Kubernetes 下的分布式压测标准流程，覆盖：

- 环境准备与健康检查
- k6-operator 方式压测
- 高压阶段（极限测试）建议流程
- 指标解释与瓶颈定位入口
- 常见故障排查（Docker Desktop / kind / kubectl）

---

## 1. 文件说明

- `deploy/k8s/loadtest/k6-search.js`：压测脚本，请求 `GET /search_http?q=...`，并输出 JSON summary。
- `deploy/k8s/loadtest/testrun.sample.yaml`：`TestRun` 静态示例。
- `scripts/k6-distributed.ps1`：PowerShell 一键脚本（安装 operator、发起测试、看状态、看日志、删除资源）。
- `deploy/k8s/loadtest/PERF_REPORT_2026-02-27.md`：一次真实的三 worker 压测结果和瓶颈结论。

---

## 2. 前置条件

### 2.1 工具版本

- `kubectl` 可用，且 context 指向目标集群。
- `Docker Desktop` 已启动（Windows 场景）。
- `kind`（如果你用 kind 做本地多节点集群）。

### 2.2 集群和服务健康

执行以下检查，确保基础服务正常再压测：

```powershell
kubectl config current-context
kubectl get nodes -o wide
kubectl -n radic get pods -o wide
kubectl -n radic get deploy,statefulset
```

建议确认这些组件都为 `Running/Ready`：

- `gateway`
- `indexer`（`statefulset`，通常 3 个分片副本）
- `redis`
- `mysql`
- `kafka`
- `zookeeper`
- `etcd`

---

## 3. 安装 k6 operator

```powershell
.\scripts\k6-distributed.ps1 -Mode install-operator
```

验证 CRD：

```powershell
kubectl get crd testruns.k6.io
```

---

## 4. 标准压测流程（operator 模式）

### 4.1 发起一次压测

```powershell
.\scripts\k6-distributed.ps1 -Mode run `
  -Namespace radic `
  -TestName radic-search-dist `
  -Parallelism 6 `
  -Rate 3000 `
  -Duration 3m `
  -PreAllocatedVUs 800 `
  -MaxVUs 3000 `
  -TargetBase http://gateway.radic.svc.cluster.local:8080 `
  -SearchEndpoint /search_http `
  -SearchQuery golang `
  -RequestTimeout 10s
```

### 4.2 观察执行状态

```powershell
.\scripts\k6-distributed.ps1 -Mode status -Namespace radic -TestName radic-search-dist
kubectl -n radic get testrun radic-search-dist -w
```

### 4.3 查看日志

```powershell
.\scripts\k6-distributed.ps1 -Mode logs -Namespace radic -TestName radic-search-dist
```

### 4.4 清理

```powershell
.\scripts\k6-distributed.ps1 -Mode delete -Namespace radic -TestName radic-search-dist
```

---

## 5. 极限压测建议（分档递增）

不要一上来直接 `15000+`，建议分档：

- 第一档：`3000`、`6000`
- 第二档：`9000`、`12000`
- 第三档：`15000`、`18000`（高风险）

每档至少跑 `45s~120s`，并记录：

- `achieved_rps`
- `http_req_failed_rate`
- `p95/p99` 延迟
- 是否触发平台异常（例如 `TLS handshake timeout`）

---

## 6. 参数含义（最重要）

- `Parallelism`：k6 runner pod 数量（并发压测进程数，不等于 VU）。
- `Rate`：目标请求速率（总目标，不是单 pod）。
- `Duration`：持续压测时长。
- `PreAllocatedVUs`：预分配虚拟用户，太小会跟不上目标速率。
- `MaxVUs`：VU 上限，达到上限后可能压不动目标 `Rate`。
- `RequestTimeout`：单请求超时，超时会计入失败或高延迟。
- `PARSE_JSON`：是否解析响应 JSON（默认关闭，避免额外客户端 CPU 干扰）。

---

## 7. 如何判断瓶颈在“业务”还是“平台”

### 7.1 业务热点信号

- 失败率低但 `p95/p99` 快速升高（例如从几十毫秒涨到几百/上千毫秒）。
- Gateway/Indexer 进程 CPU 飙高，pprof 热点集中在应用函数。

### 7.2 平台热点信号

- `kubectl` 大量报错：`net/http: TLS handshake timeout`
- Docker Desktop 报 `500 Internal Server Error`
- k6 job 无法按时完成、pod 启停异常

这类情况说明先撞到宿主机/容器运行时/控制面上限，而非应用代码本身。

---

## 8. 常见问题排查

### 8.1 `testruns.k6.io` 不存在

```powershell
kubectl get crd testruns.k6.io
```

不存在就重装 operator：

```powershell
.\scripts\k6-distributed.ps1 -Mode install-operator
```

### 8.2 `Unable to connect to the server: net/http: TLS handshake timeout`

优先检查：

- Docker Desktop 是否异常/卡死
- 宿主机 CPU、内存是否打满
- 压测速率是否过于激进

处理建议：

1. 降低压测档位（先回到 `6000~9000`）。
2. 重启 Docker Desktop。
3. 重新检查 `kubectl get nodes`、`kubectl -n radic get pods` 后再继续。

### 8.3 `no available index_service instances`

说明 gateway 未发现 indexer 节点，检查：

```powershell
kubectl -n radic get pods -l app=indexer -o wide
kubectl -n radic logs statefulset/indexer --tail=200
```

---

## 9. 指标解释（简版）

- `RPS/QPS`：每秒请求数，当前这个场景可近似看成同义。
- `p95/p99`：95%/99% 请求在该延迟内完成，反映尾延迟稳定性。
- `fail_rate`：失败率，重点关注是否持续上升。

经验上，吞吐达到平台边界时，通常先出现：

1. `p95/p99` 抬升  
2. `fail_rate` 增加  
3. 平台异常（K8s API、Docker、网络）

---

## 10. 推荐复测模板

每次改动后建议固定做一轮：

1. 热身：`Rate=3000`，`Duration=60s`
2. 基准：`Rate=6000`，`Duration=60s`
3. 高压：`Rate=9000`，`Duration=60s`
4. 边界：`Rate=12000`，`Duration=60s`

把结果存到 `.gotmp` 下 CSV，便于回归对比。
