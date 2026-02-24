package infra

import (
	"context"
	"my_radic/api_gateway/model"
	"my_radic/util"
	"time"
)

// StartRelay 启动后台消息转发协程
// 负责将 Outbox 表中的消息异步发送到 Kafka
func StartRelay(ctx context.Context, kafkaTopic string) {
	if ctx == nil {
		ctx = context.Background()
	}
	go func() {
		util.LogInfo("Relay service started, topic: %s", kafkaTopic)
		for {
			select {
			case <-ctx.Done():
				util.LogInfo("Relay service stopped: %v", ctx.Err())
				return
			default:
			}
			var tasks []model.Outbox
			// 1. 捞取待发送消息 (Limit 100)
			// 注意：这里直接使用本包内的全局变量 DB
			if err := DB.Where("status = ?", 0).Limit(100).Find(&tasks).Error; err != nil {
				util.LogError("Relay scan error: %v", err)
				time.Sleep(5 * time.Second) // 数据库故障避让
				continue
			}

			if len(tasks) == 0 {
				// 没活干，休息一会
				time.Sleep(1 * time.Second)
				continue
			}

			// 2. 逐条发送
			for _, task := range tasks {
				// 注意：这里直接调用本包内的 SendMessage
				// 我们假设 task.Topic 是对的，或者强制使用 config 里的 Topic
				// 如果 task.Topic 为空，可以用传入的 kafkaTopic
				targetTopic := task.Topic
				if targetTopic == "" {
					targetTopic = kafkaTopic
				}

				if err := SendMessage(targetTopic, task.Payload); err != nil {
					util.LogError("Relay send kafka error (id=%d): %v", task.ID, err)
					// 发送失败暂时跳过，等待下次轮询或增加重试计数
					// 也可以在这里做 task.Retry++
					continue
				}

				// 3. 发送成功，更新状态
				// Update 单个字段
				if err := DB.Model(&task).Update("status", 1).Error; err != nil {
					util.LogError("Relay update status error (id=%d): %v", task.ID, err)
				}
			}

			// 处理完一批，稍微歇一下，防止把 Kafka 打死
			time.Sleep(50 * time.Millisecond)
		}
	}()
}
