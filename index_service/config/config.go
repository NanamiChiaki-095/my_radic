package config

import (
	"my_radic/util"
	"os"
	"strconv"
	"strings"

	"github.com/goccy/go-yaml"
)

type EtcdConf struct {
	Endpoints []string `yaml:"endpoints"`
}

type KafkaConf struct {
	Endpoints []string `yaml:"endpoints"`
	Topic     string   `yaml:"topic"`
}

type Config struct {
	Etcd         EtcdConf  `yaml:"etcd"`
	Kafka        KafkaConf `yaml:"kafka"`
	DataDir      string    `yaml:"data_dir"`
	DBType       string    `yaml:"db_type"`
	RpcPort      int       `yaml:"rpc_port"`
	ShardID      int       `yaml:"shard_id"`
	TotalShards  int       `yaml:"total_shards"`
	Role         string    `yaml:"role"`
	PrimaryAddr  string    `yaml:"primary_addr"`
	ReplicaAddrs []string  `yaml:"replica_addrs"`
}

var Conf *Config

func InitConfig(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		util.LogError("InitConfig err: %v", err)
		return err
	}

	Conf = &Config{}
	err = yaml.Unmarshal(data, Conf)
	if err != nil {
		util.LogError("InitConfig err: %v", err)
		return err
	}

	util.LogInfo("Config loaded from %s", path)
	applyEnvOverrides(Conf)
	return nil
}

func applyEnvOverrides(conf *Config) {
	if conf == nil {
		return
	}

	overrideInt := func(envKey string, target *int) {
		raw := strings.TrimSpace(os.Getenv(envKey))
		if raw == "" {
			return
		}
		value, err := strconv.Atoi(raw)
		if err != nil {
			util.LogWarn("Ignore invalid %s=%q: %v", envKey, raw, err)
			return
		}
		*target = value
		util.LogInfo("Config override from env: %s=%d", envKey, value)
	}

	overrideInt("RADIC_SHARD_ID", &conf.ShardID)
	overrideInt("RADIC_TOTAL_SHARDS", &conf.TotalShards)
}
