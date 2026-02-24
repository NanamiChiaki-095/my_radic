package config

import (
	"my_radic/util"
	"os"

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
	return nil
}
