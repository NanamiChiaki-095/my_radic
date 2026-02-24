package model

import "time"

type Article struct {
	ID        uint64    `gorm:"primaryKey;autoIncrement"`
	Title     string    `gorm:"type:varchar(255);not null"`
	Content   string    `gorm:"type:longtext"`
	Url       string    `gorm:"type:varchar(512)"`
	Author    string    `gorm:"type:varchar(64)"`
	CreatedAt time.Time `gorm:"autoCreateTime"`
	UpdatedAt time.Time `gorm:"autoUpdateTime"`
}

func (Article) TableName() string {
	return "articles"
}
