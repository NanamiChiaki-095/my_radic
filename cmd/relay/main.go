package main

import (
	"context"
	"flag"
	"my_radic/api_gateway/config"
	"my_radic/api_gateway/infra"
	"my_radic/util"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	configPath := flag.String("config", "api_gateway/config/gateway_config.yaml", "Path to config file")
	flag.Parse()

	if err := config.InitConfig(*configPath); err != nil {
		util.LogError("Failed to load config: %v", err)
		os.Exit(1)
	}

	if err := infra.InitKafka(config.Conf.Kafka.Endpoints); err != nil {
		util.LogError("Failed to init kafka: %v", err)
		os.Exit(1)
	}
	defer infra.CloseKafka()

	if err := infra.InitMySQL(config.Conf.MySQL); err != nil {
		util.LogError("Failed to init mysql: %v", err)
		os.Exit(1)
	}
	defer infra.CloseMySQL()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	infra.StartRelay(ctx, config.Conf.Kafka.Topic)
	util.LogInfo("Relay started")

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	<-ch

	cancel()
	util.LogInfo("Relay shutdown")
}
