package main

import (
	"flag"
	"fmt"
	"my_radic/index_service"
	"my_radic/index_service/config"
	"my_radic/util"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func main() {
	configPath := flag.String("config", "./index_service/config/config.yaml", "Path to config file")
	flag.Parse()

	util.InitLogger("")

	// 1. 初始化配置
	if err := config.InitConfig(*configPath); err != nil {
		panic(err)
	}

	if err := os.MkdirAll(config.Conf.DataDir, 0755); err != nil {
		panic(err)
	}

	// 2. 监听配置中的端口
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", config.Conf.RpcPort))
	if err != nil {
		panic(err)
	}
	server := grpc.NewServer()

	// 注册官方健康检查服务
	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(server, healthServer)
	// 初始状态设为 NOT_SERVING，直到初始化完成
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)

	worker := &index_service.IndexServiceWorker{}

	// 3. 将配置参数传给 Init (初始化数据库、Kafka 等)
	if err := worker.Init(1000, config.Conf.DataDir, config.Conf.Kafka.Endpoints, config.Conf.Kafka.Topic, config.Conf.DBType, config.Conf.ShardID, config.Conf.TotalShards, config.Conf); err != nil {
		util.LogError("Worker Init failed: %v", err)
		panic(err)
	}

	index_service.RegisterIndexServiceServer(server, worker)

	// 4. 服务注册到 Etcd
	if err := worker.Register(config.Conf.Etcd.Endpoints, config.Conf.RpcPort); err != nil {
		util.LogError("Failed to register service: %v", err)
		panic(err)
	}

	// 初始化和注册全部成功后，再将健康状态置为 SERVING
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	healthServer.SetServingStatus("index_service", grpc_health_v1.HealthCheckResponse_SERVING)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-c
		util.LogInfo("Received signal %s, shutting down...", sig.String())

		// 关机前先标记为不可用，通知 LB (负载均衡器) 停止导流
		healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)

		server.GracefulStop()

		if err := worker.Close(); err != nil {
			util.LogError("Failed to close worker: %v", err)
		}

		util.CloseLogger()
		util.LogInfo("Shutdown complete")
		os.Exit(0)
	}()

	util.LogInfo("Starting IndexService on port %d...", config.Conf.RpcPort)
	if err := server.Serve(lis); err != nil {
		panic(err)
	}
}
