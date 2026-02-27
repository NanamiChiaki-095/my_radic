package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"my_radic/api_gateway/config"
	"my_radic/api_gateway/infra"
	"my_radic/api_gateway/router"
	"my_radic/api_gateway/rpc"
	"my_radic/util"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"time"
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
	pprofAddr := flag.String("pprof", "", "pprof listen address, e.g. :6060 (empty to disable)")
	pprofMutexFraction := flag.Int("pprof-mutex-fraction", 5, "mutex profile fraction for pprof (0 to disable)")
	pprofBlockRate := flag.Int("pprof-block-rate", 1, "block profile rate for pprof (0 to disable)")

	flag.Parse()
	util.SetServiceName("api_gateway")
	if err := util.InitLogger(filepath.Join("logs", "api_gateway.jsonl")); err != nil {
		fmt.Fprintf(os.Stderr, "init logger failed: %v\n", err)
	}
	defer util.CloseLogger()
	util.LogInfo("port: %d, configPath: %s", *port, *configPath)

	if err := loadConfig(*configPath); err != nil {
		util.LogError("Failed to load config: %v", err)
		return
	}

	// 1. 初始化基础设施
	setupInfra()
	defer closeInfra()
	startPprofServer("api_gateway", *pprofAddr, *pprofMutexFraction, *pprofBlockRate)

	// 2. 初始化 RPC 客户端
	if err := rpc.Init(config.Conf.Etcd.Endpoints); err != nil {
		util.LogError("Failed to init rpc: %v", err)
		return
	}

	// 3. 构造路由并启动
	r := router.NewRouter()
	router.SetReady(true)

	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", *port),
		Handler:           r,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		util.LogInfo("Starting API Gateway on port %d...", *port)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		util.LogInfo("Received signal %s, shutting down...", sig.String())
	case err := <-errCh:
		if err != nil {
			util.LogError("HTTP server stopped: %v", err)
		} else {
			util.LogInfo("HTTP server stopped")
		}
	}

	router.SetReady(false)
	time.Sleep(1 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	wsCtx, wscancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer wscancel()
	closed := router.CloseAllWebSockets(wsCtx)
	util.LogInfo("Closed %d WebSockets", closed)
	if err := srv.Shutdown(ctx); err != nil {
		util.LogError("Http server shutdown failed: %v", err)
	}
}

func startPprofServer(service, addr string, mutexFraction, blockRate int) {
	if addr == "" {
		return
	}
	if mutexFraction >= 0 {
		runtime.SetMutexProfileFraction(mutexFraction)
	}
	if blockRate >= 0 {
		runtime.SetBlockProfileRate(blockRate)
	}
	go func() {
		util.LogInfo("%s pprof listening on %s", service, addr)
		if err := http.ListenAndServe(addr, nil); err != nil {
			util.LogError("%s pprof server stopped: %v", service, err)
		}
	}()
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
