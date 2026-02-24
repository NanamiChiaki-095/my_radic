package main

import (
	"flag"
	"fmt"
	"my_radic/api_gateway/config"
	"my_radic/api_gateway/infra"
	"my_radic/api_gateway/router"
	"my_radic/api_gateway/rpc"
	"my_radic/util"
)

func loadConfig(configPath string) error {
	if err := config.InitConfig(configPath); err != nil {
		return err
	}
	util.LogInfo("Config loaded from %s", configPath)
	return nil
}

func main() {
	port := flag.Int("port", 8080, "API Gateway port")
	configPath := flag.String("config", "api_gateway/config/gateway_config.yaml", "Path to config file")

	flag.Parse()
	util.LogInfo("port: %d, configPath: %s", *port, *configPath)

	if err := loadConfig(*configPath); err != nil {
		util.LogError("Failed to load config: %v", err)
		return
	}

	// 1. 初始化基础设施
	setupInfra()
	defer closeInfra()

	// 2. 初始化 RPC 客户端
	if err := rpc.Init(config.Conf.Etcd.Endpoints); err != nil {
		util.LogError("Failed to init rpc: %v", err)
		return
	}

	// 3. 构造路由并启动
	r := router.NewRouter()

	util.LogInfo("Starting API Gateway on port %d...", *port)
	if err := r.Run(fmt.Sprintf(":%d", *port)); err != nil {
		util.LogError("Failed to start server: %v", err)
	}
}

func setupInfra() {
	if err := infra.InitKafka(config.Conf.Kafka.Endpoints); err != nil {
		util.LogError("Failed to init kafka: %v", err)
		panic(err)
	}

	if err := infra.InitRedis(config.Conf.Redis); err != nil {
		util.LogError("Failed to init redis: %v", err)
	}

	if err := infra.InitMySQL(config.Conf.MySQL); err != nil {
		util.LogError("Failed to init mysql: %v", err)
		panic(err)
	}

	// 启动后台消息中继
	// Relay service runs as dedicated cmd/relay.
}

func closeInfra() {
	infra.CloseKafka()
	infra.CloseRedis()
	infra.CloseMySQL()
}
