package index_service

import (
	"context"
	"my_radic/util"
	"path"
	sync "sync"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

type ServiceHub struct {
	client    *clientv3.Client //Etcd客户端
	heartbeat int64            //心跳频率
	ctx       context.Context  //上下文
	cancel    context.CancelFunc

	closeOnce sync.Once
}

var s *ServiceHub
var initOnce sync.Once

func GetServiceHub(etcdServers []string, heartbeat int64) *ServiceHub {
	initOnce.Do(func() {
		s = &ServiceHub{}
		ctx, cancel := context.WithCancel(context.Background())
		client, err := clientv3.New(clientv3.Config{
			Endpoints: etcdServers,
			DialOptions: []grpc.DialOption{
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithBlock(),
			},
			DialTimeout: 5 * time.Second,
		})

		if err != nil {
			util.LogError("GetServiceHub err: %v", err)
			panic(err)
		}
		s.client = client
		s.heartbeat = heartbeat
		s.ctx = ctx
		s.cancel = cancel
	})
	return s
}

func (s *ServiceHub) Close() {
	s.closeOnce.Do(func() {
		s.client.Close()
		s.cancel()
	})
}

func (s *ServiceHub) Register(key string, value string, leaseID clientv3.LeaseID) (clientv3.LeaseID, error) {
	util.LogInfo("Register key: %s, value: %s", key, value)
	//创建租约
	if leaseID <= 0 {
		grant, err := s.client.Grant(s.ctx, s.heartbeat)
		if err != nil {
			util.LogError("Register err: %v", err)
			return 0, err
		}
		leaseID = grant.ID
		ch, err := s.client.KeepAlive(s.ctx, leaseID)
		if err != nil {
			return 0, err
		}
		go func() {
			for resp := range ch {
				util.LogDebug("Lease %x renewed, ttl: %d", resp.ID, resp.TTL)
			}
			util.LogWarn("Lease %x lost", leaseID)
		}()
	}

	Key := path.Join("/radic", key)
	Value := value
	_, err := s.client.Put(s.ctx, Key, Value, clientv3.WithLease(leaseID))
	if err != nil {
		return 0, err
	}
	return leaseID, nil
}

func (s *ServiceHub) GetServiceSpec(serviceName string) ([]*ServiceNodeInfo, error) {
	util.LogInfo("GetServiceSpec serviceName: %s", serviceName)
	prefix := path.Join("/radic", serviceName)

	resp, err := s.client.Get(s.ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		util.LogError("GetServiceSpec err: %v", err)
		return nil, err
	}

	var nodes []*ServiceNodeInfo
	for _, kv := range resp.Kvs {
		node := &ServiceNodeInfo{}
		err := proto.Unmarshal(kv.Value, node)
		if err != nil {
			util.LogError("GetServiceSpec err: %v", err)
			continue
		}
		nodes = append(nodes, node)
	}
	return nodes, nil
}

func (s *ServiceHub) CampaignLeader(ctx context.Context, key, value string, ttl int64, onLeader func(), onFollower func()) error {
	key = path.Join("/radic", key)
	if ttl <= 0 {
		ttl = s.heartbeat
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		grant, err := s.client.Grant(ctx, ttl)
		if err != nil {
			time.Sleep(time.Second)
			util.LogError("CampaignLeader err: %v", err)
			continue
		}
		util.LogInfo("CampaignLeader key: %s, value: %s, leaseID: %d", key, value, grant.ID)

		txn := s.client.Txn(ctx).If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
			Then(clientv3.OpPut(key, value, clientv3.WithLease(grant.ID)))
		resp, err := txn.Commit()
		if err != nil {
			time.Sleep(time.Second)
			util.LogError("CampaignLeader err: %v", err)
			continue
		}
		if resp.Succeeded {
			util.LogInfo("CampaignLeader key: %s, value: %s, leaseID: %d success", key, value, grant.ID)
			if onLeader != nil {
				onLeader()
			}
			ch, err := s.client.KeepAlive(ctx, grant.ID)
			if err != nil {
				_, _ = s.client.Revoke(context.Background(), grant.ID)
				if onFollower != nil {
					onFollower()
				}
				time.Sleep(time.Second)
				continue
			}

			for {
				select {
				case <-ctx.Done():
					_, _ = s.client.Revoke(context.Background(), grant.ID)
					return ctx.Err()
				case ka, ok := <-ch:
					if !ok || ka == nil {
						util.LogWarn("Lease %x lost", grant.ID)
						if onFollower != nil {
							onFollower()
						}
						goto retry
					}
				}
			}
		}

		if onFollower != nil {
			onFollower()
		}

		getResp, err := s.client.Get(ctx, key)
		if err != nil {
			time.Sleep(time.Second)
			util.LogError("CampaignLeader err: %v", err)
			continue
		}
		watchRev := getResp.Header.Revision + 1

		wch := s.client.Watch(ctx, key, clientv3.WithRev(watchRev))
		for wresp := range wch {
			if wresp.Canceled {
				break
			}
			for _, ev := range wresp.Events {
				if ev.Type == mvccpb.DELETE {
					goto retry
				}
			}
		}
	retry:
	}
}
