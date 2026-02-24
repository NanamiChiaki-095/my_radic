package model

import "time"

type Outbox struct {
	ID       uint64    `gorm:"primaryKey;autoIncrement"`
	Topic    string    `gorm:"type:varchar(255);not null"`
	Payload  []byte    `gorm:"type:longblob"`
	Status   int       `gorm:"type:tinyint;default:0;index"` // 0:Pending, 1:Sent, 2:Failed
	Retry    int       `gorm:"default:0"`
	CreateAt time.Time `gorm:"autoCreateTime"`
}

func (Outbox) TableName() string {
	return "outbox"
}
