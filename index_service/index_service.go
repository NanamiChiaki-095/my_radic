package index_service

import (
	context "context"
	"errors"
	"my_radic/internal/indexer"
	"my_radic/internal/kvdb"
	reverseindex "my_radic/internal/reverse_index"
	"my_radic/types"
	"my_radic/util"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"my_radic/index_service/config"

	"github.com/IBM/sarama"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	status "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// IndexServiceWorker 是 gRPC 服务的具体实现类。
// 它组合了底层的 Indexer（核心索引逻辑），通过 gRPC 接口暴露给外部调用。
type IndexServiceWorker struct {
	// 必须嵌入这个结构体，以符合 gRPC 的接口标准（向前兼容）
	UnimplementedIndexServiceServer

	// Indexer 是核心组件，包含正排索引（BoltDB）和倒排索引（SkipList）
	Indexer *indexer.Indexer

	Hub           *ServiceHub
	selfAddr      string
	KafkaConsumer sarama.ConsumerGroup
	ShardID       int32
	TotalShards   int32
	Role          string   // 节点角色，"primary" 或 "replica"
	PrimaryAddr   string   // 主节点地址
	ReplicaAddrs  []string // 从节点地址列表
	CurrentSeq    uint64   // 主分片最新seq
	AppliedSeq    uint64   // 副本已应用seq

	replicaMu      sync.Mutex
	replicaClients map[string]IndexServiceClient
	replicaConns   map[string]*grpc.ClientConn

	checkpointPath string
	wal            *kvdb.MmapWAL
	checkpointStop chan struct{}
	checkpointDone chan struct{}
}

// Init 初始化索引服务 Worker。
func (s *IndexServiceWorker) Init(docNumEstimate int, dbPath string, kafkaAddrs []string, topic string, dbType string, shardID int, totalShards int, cfg *config.Config) error {
	util.LogInfo("IndexerServiceWorker Init docNumEstimate: %d, dbPath: %s, kafka: %v, topic: %s, dbType: %s", docNumEstimate, dbPath, kafkaAddrs, topic, dbType)

	var forwardIndex kvdb.KVStore

	s.Role = cfg.Role
	s.PrimaryAddr = cfg.PrimaryAddr
	s.ReplicaAddrs = cfg.ReplicaAddrs
	if dbType == "badger" {
		// 初始化 BadgerDB
		// Badger 在 Windows 上对路径分隔符比较敏感，最好用 filepath.Join
		// 另外 Badger 是存目录的，不是单文件
		badgerPath := filepath.Join(dbPath, "badger")
		bd := kvdb.NewBadger().WithDataPath(badgerPath)
		if err := bd.Open(); err != nil {
			util.LogError("IndexerServiceWorker Init BadgerDB err: %v", err)
			return err
		}
		forwardIndex = bd
	} else {
		// 初始化 BoltDB (默认)
		realPath := filepath.Join(dbPath, "radic.db")
		boltDB := kvdb.NewBolt().WithDataPath(realPath)
		if err := boltDB.WithBucket("doc_store").Open(); err != nil {
			util.LogError("IndexerServiceWorker Init boltDB err: %v", err)
			return err
		}
		forwardIndex = boltDB
	}

	// 加载检查点
	checkpointPath := filepath.Join(dbPath, "checkpoint")
	checkpointExists := true
	if _, statErr := os.Stat(checkpointPath); statErr != nil {
		if !os.IsNotExist(statErr) {
			util.LogError("IndexerServiceWorker Init checkpoint stat err: %v", statErr)
			return statErr
		}
		checkpointExists = false
	}
	offset, err := util.LoadCheckpoint(checkpointPath)
	if err != nil {
		util.LogError("IndexerServiceWorker Init LoadCheckpoint err: %v", err)
		return err
	}
	util.LogInfo("IndexerServiceWorker Init LoadCheckpoint success, offset: %d", offset)

	// 初始化 Kafka 消费者组
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	config.Version = sarama.V2_6_0_0

	consumerGroup, err := sarama.NewConsumerGroup(kafkaAddrs, "index_service_group", config)
	if err != nil {
		util.LogError("IndexerServiceWorker Init kafka consumerGroup err: %v", err)
		return err
	}
	s.KafkaConsumer = consumerGroup

	// 2. 初始化 SkipListReverseIndex (倒排索引)
	walPath := filepath.Join(dbPath, "reverse.wal")
	wal, err := kvdb.OpenMmapWAL(walPath, 0, 2*time.Second)
	if err != nil {
		util.LogError("IndexerServiceWorker Init wal err: %v", err)
		return err
	}
	// 启动恢复阶段先禁用 WAL，避免 LoadFromForwardIndex / ReplayWALFrom 反向写入 WAL，
	// 导致每次重启都产生大量重复日志和重复回放。
	revIdx := reverseindex.NewSkipListReverseIndex(docNumEstimate)

	// 3. 组装 Indexer
	s.Indexer = indexer.NewIndexer(forwardIndex, revIdx)

	count, err := s.Indexer.LoadFromForwardIndex()
	if err != nil {
		util.LogError("IndexerServiceWorker Init LoadFromForwardIndex err: %v", err)
		_ = s.Indexer.Close()
		return err
	}
	util.LogInfo("IndexerServiceWorker Init LoadFromForwardIndex success, count: %d", count)
	shouldReplay := checkpointExists && !(offset == 0 && count > 0)
	if shouldReplay {
		lastOffset, replayErr := s.Indexer.ReplayWALFrom(wal, offset)
		if replayErr != nil {
			util.LogError("IndexerServiceWorker Init ReplayWALFrom err: %v", replayErr)
			_ = s.Indexer.Close()
			return replayErr
		}
		if saveErr := util.SaveCheckpoint(checkpointPath, lastOffset); saveErr != nil {
			util.LogError("IndexerServiceWorker Init SaveCheckpoint err: %v", saveErr)
			_ = s.Indexer.Close()
			return saveErr
		}
		util.LogInfo("IndexerServiceWorker Init ReplayWALFrom success, lastOffset: %d", lastOffset)
	} else {
		if checkpointExists && offset == 0 && count > 0 {
			util.LogWarn("IndexerServiceWorker Init checkpoint offset is 0 with non-empty forward index, skip WAL replay")
		} else {
			util.LogWarn("IndexerServiceWorker Init checkpoint missing, skip WAL replay on this boot")
		}
		lastOffset := wal.CurrentSeq()
		if saveErr := util.SaveCheckpoint(checkpointPath, lastOffset); saveErr != nil {
			util.LogError("IndexerServiceWorker Init SaveCheckpoint err: %v", saveErr)
			_ = s.Indexer.Close()
			return saveErr
		}
		util.LogInfo("IndexerServiceWorker Init checkpoint refreshed, lastOffset: %d", lastOffset)
	}

	s.checkpointPath = checkpointPath
	s.wal = wal
	s.startCheckpointLoop(5 * time.Second)

	// 恢复完成后再启用 WAL，后续在线写入才会落盘。
	revIdx.EnableWALWithMode(wal, 1024, false)

	// 初始化分片信息
	s.ShardID = int32(shardID)
	s.TotalShards = int32(totalShards)

	// 启动后台消费协程
	ctx := context.Background()
	go func() {
		handler := &ConsumerHandler{
			indexer: s.Indexer,
		}
		for {
			err := s.KafkaConsumer.Consume(ctx, []string{topic}, handler)
			if err != nil {
				util.LogError("IndexerServiceWorker kafka Consume err: %v", err)
			}
			// 防止异常情况下死循环空转
			time.Sleep(2 * time.Second)
		}
	}()

	return nil
}

// Close 关闭服务，释放资源（如关闭数据库连接）。
func (s *IndexServiceWorker) Close() error {
	s.stopCheckpointLoop()
	s.persistCheckpoint()

	if s.KafkaConsumer != nil {
		_ = s.KafkaConsumer.Close()
	}

	s.replicaMu.Lock()
	for _, conn := range s.replicaConns {
		_ = conn.Close()
	}
	s.replicaMu.Unlock()

	var closeErr error
	if s.Indexer != nil {
		closeErr = s.Indexer.Close()
	}
	s.wal = nil
	return closeErr
}

func (s *IndexServiceWorker) persistCheckpoint() {
	if s.wal == nil || s.checkpointPath == "" {
		return
	}
	if err := util.SaveCheckpoint(s.checkpointPath, s.wal.CurrentSeq()); err != nil {
		util.LogWarn("IndexerServiceWorker persistCheckpoint err: %v", err)
	}
}

func (s *IndexServiceWorker) startCheckpointLoop(interval time.Duration) {
	if s.wal == nil || s.checkpointPath == "" {
		return
	}
	if interval <= 0 {
		interval = 5 * time.Second
	}
	s.stopCheckpointLoop()
	s.checkpointStop = make(chan struct{})
	s.checkpointDone = make(chan struct{})
	go func(stop <-chan struct{}, done chan<- struct{}) {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				s.persistCheckpoint()
			case <-stop:
				return
			}
		}
	}(s.checkpointStop, s.checkpointDone)
}

func (s *IndexServiceWorker) stopCheckpointLoop() {
	if s.checkpointStop == nil {
		return
	}
	close(s.checkpointStop)
	if s.checkpointDone != nil {
		<-s.checkpointDone
	}
	s.checkpointStop = nil
	s.checkpointDone = nil
}

// AddDoc 处理添加单条文档的请求。
func (s *IndexServiceWorker) AddDoc(ctx context.Context, req *AddDocRequest) (*AddDocResponse, error) {
	util.LogInfo("IndexerServiceWorker AddDoc req: %v", req)

	// 参数校验：防止空指针 Panic
	if req == nil || req.Doc == nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid request")
	}
	if s.Role != "primary" && !isReplicaWrite(ctx) {
		return nil, errors.New("replicas reject write")
	}

	var replicaDoc *types.Document
	if s.Role == "primary" {
		if cloned := proto.Clone(req.Doc); cloned != nil {
			replicaDoc, _ = cloned.(*types.Document)
		}
	}

	// 调用底层 Indexer 添加文档，获取生成的内部 ID
	intId, err := s.Indexer.AddDocument(req.Doc)
	if err != nil {
		util.LogError("IndexerServiceWorker AddDoc err: %v", err)
		return &AddDocResponse{DocId: 0, Success: false}, err
	}

	if s.Role == "primary" && replicaDoc != nil {
		go s.replicateAddDoc(replicaDoc)
	}
	return &AddDocResponse{DocId: intId, Success: true}, nil
}

// DeleteDoc 处理删除单条文档的请求。
func (s *IndexServiceWorker) DeleteDoc(ctx context.Context, req *DeleteDocRequest) (*DeleteDocResponse, error) {
	util.LogInfo("IndexerServiceWorker DeleteDoc req: %v", req)

	if req == nil || req.DocId == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "invalid request")
	}
	if s.Role != "primary" && !isReplicaWrite(ctx) {
		return nil, errors.New("replicas reject write")
	}

	// 从正排和倒排中移除文档
	if err := s.Indexer.DeleteDocument(req.DocId); err != nil {
		util.LogError("IndexerServiceWorker DeleteDoc err: %v", err)
		return &DeleteDocResponse{Success: false}, err
	}
	if s.Role == "primary" {
		go s.replicateDeleteDoc(req.DocId)
	}
	return &DeleteDocResponse{Success: true}, nil
}

// Search 处理搜索请求。
// 支持关键词查询以及 AND/OR 组合的复杂查询树。
func (s *IndexServiceWorker) Search(ctx context.Context, req *SearchRequest) (*SearchResponse, error) {
	util.LogInfo("IndexerServiceWorker Search req: %v", req)

	if req == nil || req.Query == nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid request")
	}

	// 调用 SearchComplex 递归处理查询树
	docs, err := s.Indexer.SearchComplex(req.Query)
	if err != nil {
		util.LogError("IndexerServiceWorker Search err: %v", err)
		return nil, err
	}

	return &SearchResponse{
		Results: docs,
		Total:   int32(len(docs)),
	}, nil
}

// BatchAddDoc 批量添加文档。
// 相比单条添加，批量操作能减少网络开销和磁盘 I/O 事务次数。
func (s *IndexServiceWorker) BatchAddDoc(ctx context.Context, req *BatchAddDocRequest) (*BatchAddDocResponse, error) {
	util.LogInfo("IndexerServiceWorker BatchAddDoc req: %v", req)

	if req == nil || len(req.Docs) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "invalid request")
	}
	if s.Role != "primary" && !isReplicaWrite(ctx) {
		return nil, errors.New("replicas reject write")
	}

	var replicaDocs []*types.Document
	if s.Role == "primary" {
		replicaDocs = make([]*types.Document, 0, len(req.Docs))
		for _, doc := range req.Docs {
			if doc == nil {
				continue
			}
			if cloned := proto.Clone(doc); cloned != nil {
				if d, ok := cloned.(*types.Document); ok {
					replicaDocs = append(replicaDocs, d)
				}
			}
		}
	}

	docIds, err := s.Indexer.BatchAddDocument(req.Docs)
	if err != nil {
		util.LogError("IndexerServiceWorker BatchAddDoc err: %v", err)
		return nil, err
	}
	if s.Role == "primary" && len(replicaDocs) > 0 {
		go s.replicateBatchAddDoc(replicaDocs)
	}

	return &BatchAddDocResponse{
		DocIds:  docIds,
		Success: true,
	}, nil
}

// BatchDeleteDoc 批量删除文档。
func (s *IndexServiceWorker) BatchDeleteDoc(ctx context.Context, req *BatchDeleteDocRequest) (*BatchDeleteDocResponse, error) {
	util.LogInfo("IndexerServiceWorker BatchDeleteDoc req: %v", req)

	if req == nil || len(req.DocIds) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "invalid request")
	}
	if s.Role != "primary" && !isReplicaWrite(ctx) {
		return nil, errors.New("replicas reject write")
	}

	if err := s.Indexer.BatchDeleteDocument(req.DocIds); err != nil {
		util.LogError("IndexerServiceWorker BatchDeleteDoc err: %v", err)
		return nil, err
	}
	if s.Role == "primary" {
		go s.replicateBatchDeleteDoc(req.DocIds)
	}
	return &BatchDeleteDocResponse{
		Success: true,
		Count:   int32(len(req.DocIds)),
	}, nil
}

func isReplicaWrite(ctx context.Context) bool {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return false
	}
	values := md.Get("x-replica-write")
	return len(values) > 0 && values[0] == "1"
}

func (s *IndexServiceWorker) initReplicaClients() error {
	if len(s.ReplicaAddrs) == 0 {
		return nil
	}
	s.replicaMu.Lock()
	defer s.replicaMu.Unlock()
	if s.replicaClients == nil {
		s.replicaClients = make(map[string]IndexServiceClient)
	}
	if s.replicaConns == nil {
		s.replicaConns = make(map[string]*grpc.ClientConn)
	}
	for _, addr := range s.ReplicaAddrs {
		if addr == "" {
			continue
		}
		if _, ok := s.replicaClients[addr]; ok {
			continue
		}
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		s.replicaConns[addr] = conn
		s.replicaClients[addr] = NewIndexServiceClient(conn)
	}
	return nil
}

func (s *IndexServiceWorker) replicaClientsSnapshot() []IndexServiceClient {
	s.replicaMu.Lock()
	defer s.replicaMu.Unlock()
	if len(s.replicaClients) == 0 {
		return nil
	}
	clients := make([]IndexServiceClient, 0, len(s.replicaClients))
	for _, c := range s.replicaClients {
		clients = append(clients, c)
	}
	return clients
}

func (s *IndexServiceWorker) replicateAddDoc(doc *types.Document) {
	if s.Role != "primary" {
		return
	}
	if err := s.initReplicaClients(); err != nil {
		util.LogError("replicateAddDoc initReplicaClients err: %v", err)
		return
	}
	clients := s.replicaClientsSnapshot()
	if len(clients) == 0 {
		return
	}
	md := metadata.Pairs("x-replica-write", "1")
	for _, client := range clients {
		ctx, cancel := context.WithTimeout(metadata.NewOutgoingContext(context.Background(), md), 2*time.Second)
		_, err := client.AddDoc(ctx, &AddDocRequest{Doc: doc})
		cancel()
		if err != nil {
			util.LogError("replicateAddDoc err: %v", err)
		}
	}
}

func (s *IndexServiceWorker) replicateDeleteDoc(docId uint64) {
	if s.Role != "primary" {
		return
	}
	if err := s.initReplicaClients(); err != nil {
		util.LogError("replicateDeleteDoc initReplicaClients err: %v", err)
		return
	}
	clients := s.replicaClientsSnapshot()
	if len(clients) == 0 {
		return
	}
	md := metadata.Pairs("x-replica-write", "1")
	for _, client := range clients {
		ctx, cancel := context.WithTimeout(metadata.NewOutgoingContext(context.Background(), md), 2*time.Second)
		_, err := client.DeleteDoc(ctx, &DeleteDocRequest{DocId: docId})
		cancel()
		if err != nil {
			util.LogError("replicateDeleteDoc err: %v", err)
		}
	}
}

func (s *IndexServiceWorker) replicateBatchAddDoc(docs []*types.Document) {
	if s.Role != "primary" || len(docs) == 0 {
		return
	}
	if err := s.initReplicaClients(); err != nil {
		util.LogError("replicateBatchAddDoc initReplicaClients err: %v", err)
		return
	}
	clients := s.replicaClientsSnapshot()
	if len(clients) == 0 {
		return
	}
	md := metadata.Pairs("x-replica-write", "1")
	for _, client := range clients {
		ctx, cancel := context.WithTimeout(metadata.NewOutgoingContext(context.Background(), md), 3*time.Second)
		_, err := client.BatchAddDoc(ctx, &BatchAddDocRequest{Docs: docs})
		cancel()
		if err != nil {
			util.LogError("replicateBatchAddDoc err: %v", err)
		}
	}
}

func (s *IndexServiceWorker) replicateBatchDeleteDoc(docIds []uint64) {
	if s.Role != "primary" || len(docIds) == 0 {
		return
	}
	if err := s.initReplicaClients(); err != nil {
		util.LogError("replicateBatchDeleteDoc initReplicaClients err: %v", err)
		return
	}
	clients := s.replicaClientsSnapshot()
	if len(clients) == 0 {
		return
	}
	md := metadata.Pairs("x-replica-write", "1")
	for _, client := range clients {
		ctx, cancel := context.WithTimeout(metadata.NewOutgoingContext(context.Background(), md), 3*time.Second)
		_, err := client.BatchDeleteDoc(ctx, &BatchDeleteDocRequest{DocIds: docIds})
		cancel()
		if err != nil {
			util.LogError("replicateBatchDeleteDoc err: %v", err)
		}
	}
}

func (s *IndexServiceWorker) Register(etcdServers []string, servicePort int) error {
	ipAddr, err := util.GetLocalIP()
	if err != nil {
		return err
	}

	s.selfAddr = ipAddr + ":" + strconv.Itoa(servicePort)
	s.Hub = GetServiceHub(etcdServers, 3)

	key := path.Join("index_service", s.selfAddr)
	info := &ServiceNodeInfo{
		ServiceAddr: s.selfAddr,
		Weight:      100,
		ShardId:     s.ShardID,
		TotalShards: s.TotalShards,
	}
	_, err = s.Hub.RegisterProto(key, info, 0)
	if err != nil {
		util.LogError("Register err: %v", err)
		return err
	}

	util.LogInfo("Register success, selfAddr: %v", s.selfAddr)
	return nil
}
