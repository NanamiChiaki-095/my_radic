package config

import (
	"my_radic/util"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

type EtcdConf struct {
	Endpoints []string `yaml:"endpoints"`
}

type KafkaConf struct {
	Endpoints []string `yaml:"endpoints"`
	Topic     string   `yaml:"topic"`
}

type RedisConf struct {
	Addr     string `yaml:"addr"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
}

type MySQLConf struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	DBName   string `yaml:"db_name"`
}

type Config struct {
	Etcd  EtcdConf  `yaml:"etcd"`
	Kafka KafkaConf `yaml:"kafka"`
	Redis RedisConf `yaml:"redis"`
	MySQL MySQLConf `yaml:"mysql"`
}

var Conf *Config

func InitConfig(filePath string) error {
	data, err := os.ReadFile(filePath)
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

	if envAddr := os.Getenv("RADIC_MYSQL_ADDR"); envAddr != "" {
		Conf.MySQL.Host = envAddr
	}
	if envPass := os.Getenv("RADIC_MYSQL_PASSWORD"); envPass != "" {
		Conf.MySQL.Password = envPass
	}
	if envUser := os.Getenv("RADIC_MYSQL_USER"); envUser != "" {
		Conf.MySQL.User = envUser
	}
	if envDB := os.Getenv("RADIC_MYSQL_DB_NAME"); envDB != "" {
		Conf.MySQL.DBName = envDB
	}
	if envPort := os.Getenv("RADIC_MYSQL_PORT"); envPort != "" {
		Conf.MySQL.Port, _ = strconv.Atoi(envPort)
	}

	if envRedisAddr := os.Getenv("RADIC_REDIS_ADDR"); envRedisAddr != "" {
		Conf.Redis.Addr = envRedisAddr
	}
	if envRedisPass := os.Getenv("RADIC_REDIS_PASSWORD"); envRedisPass != "" {
		Conf.Redis.Password = envRedisPass
	}
	if envRedisDB := os.Getenv("RADIC_REDIS_DB"); envRedisDB != "" {
		Conf.Redis.DB, _ = strconv.Atoi(envRedisDB)
	}

	if envEtcdEndpoints := os.Getenv("RADIC_ETCD_ENDPOINTS"); envEtcdEndpoints != "" {
		Conf.Etcd.Endpoints = strings.Split(envEtcdEndpoints, ",")
	}

	if envKafkaEndpoints := os.Getenv("RADIC_KAFKA_ENDPOINTS"); envKafkaEndpoints != "" {
		Conf.Kafka.Endpoints = strings.Split(envKafkaEndpoints, ",")
	}
	if envKafkaTopic := os.Getenv("RADIC_KAFKA_TOPIC"); envKafkaTopic != "" {
		Conf.Kafka.Topic = envKafkaTopic
	}

	util.LogInfo("Config loaded from %s", filePath)
	return nil
}
