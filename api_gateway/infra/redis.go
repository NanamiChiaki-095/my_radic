package infra

import (
	"context"
	"my_radic/api_gateway/config"
	"my_radic/util"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/redis/go-redis/v9"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

var RDB *redis.Client

func InitRedis(conf config.RedisConf) error {
	RDB = redis.NewClient(&redis.Options{
		Addr:     conf.Addr,
		Password: conf.Password,
		DB:       conf.DB,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := RDB.Ping(ctx).Err()
	if err != nil {
		util.LogError("Redis connect err: %v", err)
		return err
	}
	util.LogInfo("Redis connected, addr: %s", conf.Addr)
	return nil
}

func CloseRedis() error {
	return RDB.Close()
}

func SetSearchCache(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	bytes, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return RDB.Set(ctx, key, bytes, ttl).Err()
}

func GetSearchCache(ctx context.Context, key string, target interface{}) (bool, error) {
	val, err := RDB.Get(ctx, key).Result()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, json.Unmarshal([]byte(val), target)
}
