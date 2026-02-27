package rpc

import (
	"context"
	"fmt"
	"hash/crc32"
	"my_radic/index_service"
	"my_radic/types"
	"my_radic/util"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"golang.org/x/sync/singleflight"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	searchRPCTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "search_rpc_total",
			Help: "Total number of backend search RPC calls.",
		},
		[]string{"result"},
	)
	searchSingleflightSharedTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "search_singleflight_shared_total",
			Help: "Total number of search requests served via shared singleflight result.",
		},
	)
)

func init() {
	for _, collector := range []prometheus.Collector{
		searchRPCTotal,
		searchSingleflightSharedTotal,
	} {
		if err := prometheus.Register(collector); err != nil {
			if _, ok := err.(prometheus.AlreadyRegisteredError); ok {
				continue
			}
			util.LogError("prometheus.Register failed: %v", err)
		}
	}
}

// Client 是全局的 IndexService 客户端实例。
// 它封装了多节点管理、负载均衡、分片路由和聚合逻辑。
var Client index_service.IndexServiceClient

// Balancer 定义了负载均衡算法的接口。
type Balancer interface {
	// Pick 从给定的节点列表中选择一个节点进行调用（如轮询、权重或随机）。
	Pick(nodes []index_service.IndexServiceClient) index_service.IndexServiceClient
}

// RoundRobinBalancer 实现了最简单的轮询负载均衡算法。
type RoundRobinBalancer struct {
	counter uint64
}

// MultiIndexServiceClient 是分布式的 gRPC 客户端实现。
// 它实现了 index_service.IndexServiceClient 接口，隐藏了后端的复杂分片逻辑。
type MultiIndexServiceClient struct {
	mu sync.RWMutex
	// shards 存储了分片 ID 到对应的客户端列表（副本）的映射。
	// key: ShardID, value: []IndexServiceClient (Replica Set)
	shards map[int32][]index_service.IndexServiceClient
	// totalShards 记录了全系统的总分片数，用于 Scatter-Gather 搜索。
	totalShards int32
	// clients 存储了所有可用节点的平面列表，用于传统的负载均衡。
	clients []index_service.IndexServiceClient
	// balancer 负载均衡策略。
	balancer Balancer
	// sf (SingleFlight) 用于防止热点查询击穿，合并重复的并发请求。
	sf singleflight.Group

	conns map[string]*grpc.ClientConn
}

// Pick 实现轮询算法，使用原子操作保证并发安全。
func (b *RoundRobinBalancer) Pick(nodes []index_service.IndexServiceClient) index_service.IndexServiceClient {
	if len(nodes) == 0 {
		return nil
	}
	idx := atomic.AddUint64(&b.counter, 1) % uint64(len(nodes))
	return nodes[idx]
}

// Search 实现了 Scatter-Gather (分散-收集) 搜索模式。
// 逻辑流：
// 1. SingleFlight 合并重复请求。
// 2. 并发向所有分片发起搜索请求。
// 3. 汇总所有分片的 TopK 结果。
// 4. 全局合并排序，返回最终结果。
func (m *MultiIndexServiceClient) Search(ctx context.Context, in *index_service.SearchRequest, opts ...grpc.CallOption) (*index_service.SearchResponse, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// 使用请求参数构造 Key，确保相同的查询被合并
	key := fmt.Sprintf("search:%s", in.String())
	v, err, shared := m.sf.Do(key, func() (interface{}, error) {
		var allDocs []*types.Document
		var wg sync.WaitGroup
		var mutex sync.Mutex

		// 遍历所有分片
		for shardID := int32(0); shardID < m.totalShards; shardID++ {
			wg.Add(1)
			go func(sid int32) {
				defer wg.Done()

				// 从当前分片的副本集中选出一个可用节点
				replicas, ok := m.shards[sid]
				if !ok || len(replicas) == 0 {
					searchRPCTotal.WithLabelValues("unavailable").Inc()
					util.LogError("Shard %d not found or no available nodes", sid)
					return
				}

				client := m.balancer.Pick(replicas)
				// 发起 RPC 调用
				res, err := client.Search(ctx, in, opts...)
				if err != nil {
					searchRPCTotal.WithLabelValues("error").Inc()
					util.LogError("Search Shard %d err: %v", sid, err)
					return
				}
				searchRPCTotal.WithLabelValues("ok").Inc()

				// 汇总结果 (加锁保证 append 安全)
				mutex.Lock()
				allDocs = append(allDocs, res.Results...)
				mutex.Unlock()
			}(shardID)
		}
		wg.Wait()

		// 全局排序：按分数降序排列
		sort.Slice(allDocs, func(i, j int) bool {
			return allDocs[i].Score > allDocs[j].Score
		})

		// 截断逻辑：通常只返回全局 TopK (此处可根据业务增加 limit 逻辑)
		return &index_service.SearchResponse{
			Results: allDocs,
			Total:   int32(len(allDocs)),
		}, nil
	})

	if shared {
		searchSingleflightSharedTotal.Inc()
	}

	if err != nil {
		return nil, err
	}

	return v.(*index_service.SearchResponse), nil
}

// AddDoc 实现了有状态路由写入。
// 根据文档 ID 计算其所属的分片，并发送到对应的分片节点。
func (m *MultiIndexServiceClient) AddDoc(ctx context.Context, in *index_service.AddDocRequest, opts ...grpc.CallOption) (*index_service.AddDocResponse, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	targetShard := getShardID(in.Doc.Id, m.totalShards)

	clients, ok := m.shards[targetShard]
	if !ok || len(clients) == 0 {
		return nil, fmt.Errorf("shard %d not found or no available nodes", targetShard)
	}

	selectedClient := m.balancer.Pick(clients)
	return selectedClient.AddDoc(ctx, in, opts...)
}

// DeleteDoc 实现同上。
func (m *MultiIndexServiceClient) DeleteDoc(ctx context.Context, in *index_service.DeleteDocRequest, opts ...grpc.CallOption) (*index_service.DeleteDocResponse, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	docIDStr := strconv.FormatUint(in.DocId, 10)
	targetShard := getShardID(docIDStr, m.totalShards)

	clients, ok := m.shards[targetShard]
	if !ok || len(clients) == 0 {
		return nil, fmt.Errorf("shard %d not found or no available nodes", targetShard)
	}

	selectedClient := m.balancer.Pick(clients)
	return selectedClient.DeleteDoc(ctx, in, opts...)
}

// BatchAddDoc 实现了跨分片的批量写入路由逻辑。
// 逻辑流：将一批文档按所属分片分组，并发发送给各个分片。
func (m *MultiIndexServiceClient) BatchAddDoc(ctx context.Context, in *index_service.BatchAddDocRequest, opts ...grpc.CallOption) (*index_service.BatchAddDocResponse, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	// 1. 拆包分组
	groups := make(map[int32][]*types.Document)
	for _, doc := range in.Docs {
		shardID := getShardID(doc.Id, m.totalShards)
		groups[shardID] = append(groups[shardID], doc)
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	totalDocIds := []uint64{}
	success := true

	// 2. 并发分发
	for shardID, docs := range groups {
		wg.Add(1)
		go func(sid int32, ds []*types.Document) {
			defer wg.Done()

			clients, ok := m.shards[sid]
			if !ok || len(clients) == 0 {
				err := fmt.Errorf("shard %d not found", sid)
				util.LogError("%v", err)
				mu.Lock()
				success = false
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
				return
			}

			client := m.balancer.Pick(clients)
			subRes, err := client.BatchAddDoc(ctx, &index_service.BatchAddDocRequest{Docs: ds}, opts...)

			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				success = false
				if firstErr == nil {
					firstErr = err
				}
			} else if !subRes.Success {
				success = false
				if firstErr == nil {
					firstErr = fmt.Errorf("shard %d returned failure", sid)
				}
			} else {
				// 即使有部分分片失败，也收集成功的 ID
				totalDocIds = append(totalDocIds, subRes.DocIds...)
			}
		}(shardID, docs)
	}
	wg.Wait()

	return &index_service.BatchAddDocResponse{
		DocIds:  totalDocIds,
		Success: success,
	}, firstErr
}

// BatchDeleteDoc 实现同 BatchAddDoc，按 ID 进行分片分组删除。
func (m *MultiIndexServiceClient) BatchDeleteDoc(ctx context.Context, in *index_service.BatchDeleteDocRequest, opts ...grpc.CallOption) (*index_service.BatchDeleteDocResponse, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	groups := make(map[int32][]uint64)
	for _, docID := range in.DocIds {
		docIDStr := strconv.FormatUint(docID, 10)
		shardID := getShardID(docIDStr, m.totalShards)
		groups[shardID] = append(groups[shardID], docID)
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	totalCount := int32(0)
	success := true

	for shardID, ids := range groups {
		wg.Add(1)
		go func(sid int32, docIDs []uint64) {
			defer wg.Done()

			clients, ok := m.shards[sid]
			if !ok || len(clients) == 0 {
				err := fmt.Errorf("shard %d not found", sid)
				mu.Lock()
				success = false
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
				return
			}

			client := m.balancer.Pick(clients)
			subRes, err := client.BatchDeleteDoc(ctx, &index_service.BatchDeleteDocRequest{DocIds: docIDs}, opts...)

			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				success = false
				if firstErr == nil {
					firstErr = err
				}
			} else if !subRes.Success {
				success = false
				if firstErr == nil {
					firstErr = fmt.Errorf("shard %d delete failed", sid)
				}
			} else {
				totalCount += subRes.Count
			}
		}(shardID, ids)
	}
	wg.Wait()

	return &index_service.BatchDeleteDocResponse{
		Count:   totalCount,
		Success: success,
	}, firstErr
}

// Init 初始化分布式 RPC 客户端。
// 从 Etcd 获取节点信息并建立连接池。
func Init(etcdServers []string) error {
	wrapper := &MultiIndexServiceClient{
		balancer: &RoundRobinBalancer{counter: 0},
		shards:   make(map[int32][]index_service.IndexServiceClient),
		clients:  make([]index_service.IndexServiceClient, 0),
		conns:    make(map[string]*grpc.ClientConn),
	}

	// 第一次全量加载节点信息
	wrapper.reloadNodes(etcdServers)

	if len(wrapper.clients) == 0 {
		return fmt.Errorf("no available index_service instances")
	}

	// 启动后台监听
	go wrapper.watch(etcdServers)

	Client = wrapper
	util.LogInfo("RPC Client initialized with %d nodes and %d shards", len(wrapper.clients), wrapper.totalShards)
	return nil
}

// getShardID 辅助函数：根据文档 ID 计算分片 ID。
// 使用 CRC32 哈希算法确保任何字符串 ID 都能均匀分布到各个分片。
func getShardID(docID string, totalShards int32) int32 {
	if totalShards <= 0 {
		return 0
	}
	// 计算字符串的 CRC32 校验和
	checkSum := crc32.ChecksumIEEE([]byte(docID))
	// 对总分片数取模
	return int32(checkSum % uint32(totalShards))
}

func (m *MultiIndexServiceClient) reloadNodes(etcdServers []string) {
	nodes, err := index_service.GetServiceHub(etcdServers, 3).GetServiceSpec("index_service")
	if err != nil {
		util.LogError("Failed to reload nodes :%v", err)
		return
	}

	wantedAddrs := map[string]struct{}{}
	for _, node := range nodes {
		if node == nil || node.ServiceAddr == "" {
			continue
		}
		wantedAddrs[node.ServiceAddr] = struct{}{}
	}
	m.mu.RLock()
	existingConns := make(map[string]*grpc.ClientConn, len(m.conns))
	for addr, conn := range m.conns {
		existingConns[addr] = conn
	}
	m.mu.RUnlock()

	dialedConns := make(map[string]*grpc.ClientConn)
	for addr := range wantedAddrs {
		if existingConns[addr] != nil {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
		cancel()
		if err != nil {
			util.LogError("Failed to dial %s: %v", addr, err)
			continue
		}
		dialedConns[addr] = conn
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.conns == nil {
		m.conns = make(map[string]*grpc.ClientConn)
	}

	for addr, conn := range dialedConns {
		if m.conns[addr] == nil {
			m.conns[addr] = conn
		} else {
			_ = conn.Close()
		}
	}

	newShards := make(map[int32][]index_service.IndexServiceClient)
	newClients := make([]index_service.IndexServiceClient, 0, len(nodes))
	newTotalShards := int32(0)
	totalShardsSet := false

	for _, node := range nodes {
		if node == nil || node.ServiceAddr == "" {
			continue
		}
		conn := m.conns[node.ServiceAddr]
		if conn == nil {
			continue
		}

		if !totalShardsSet {
			if node.TotalShards <= 0 {
				util.LogError("invalid TotalShards detected: %d from %s", node.TotalShards, node.ServiceAddr)
				continue
			}
			newTotalShards = node.TotalShards
			totalShardsSet = true
		} else if node.TotalShards != newTotalShards {
			util.LogError("inconsistent TotalShards detected: expected %d, got %d", newTotalShards, node.TotalShards)
			continue
		}

		client := index_service.NewIndexServiceClient(conn)
		if node.ShardId < 0 || node.ShardId >= newTotalShards {
			continue
		}
		newClients = append(newClients, client)
		newShards[node.ShardId] = append(newShards[node.ShardId], client)
	}

	for addr, conn := range m.conns {
		if _, ok := wantedAddrs[addr]; !ok {
			_ = conn.Close()
			delete(m.conns, addr)
		}
	}

	m.shards = newShards
	m.clients = newClients
	m.totalShards = newTotalShards
}

func (m *MultiIndexServiceClient) watch(etcdServers []string) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   etcdServers,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		util.LogError("Failed to init etcd client: %v", err)
		return
	}
	defer cli.Close()

	watchChan := cli.Watch(context.Background(), "/radic/index_service", clientv3.WithPrefix())

	for watchResp := range watchChan {
		for _, event := range watchResp.Events {
			if event.Type == mvccpb.PUT {
				util.LogInfo("watch event: %v", event)
			} else if event.Type == mvccpb.DELETE {
				util.LogInfo("watch event: %v", event)
			}
			m.reloadNodes(etcdServers)
		}
	}
}

func Close() error {
	// 可以在此处关闭所有 gRPC 连接 (由于使用了全局 Client，通常在程序退出时统一处理)
	return nil
}
