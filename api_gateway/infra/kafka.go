package infra

import (
	"my_radic/util"

	"github.com/IBM/sarama"
)

var Producer sarama.SyncProducer

// InitKafka 初始化Kafka生产者
func InitKafka(addrs []string) error {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	p, err := sarama.NewSyncProducer(addrs, config)
	if err != nil {
		util.LogError("InitKafka err: %v", err)
		return err
	}
	Producer = p
	return nil
}

// SendMessage 发送消息到Kafka
func SendMessage(topic string, message []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(message),
	}
	_, _, err := Producer.SendMessage(msg)
	if err != nil {
		util.LogError("SendMessage err: %v", err)
		return err
	}
	return nil
}

func CloseKafka() error {
	return Producer.Close()
}
