package infra

import (
	"fmt"
	"my_radic/api_gateway/config"
	"my_radic/api_gateway/model"
	"my_radic/util"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

var DB *gorm.DB

func InitMySQL(conf config.MySQLConf) error {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		conf.User, conf.Password, conf.Host, conf.Port, conf.DBName)
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		util.LogError("InitMySQL err: %v", err)
		return err
	}
	if err := db.AutoMigrate(&model.Article{}, &model.Outbox{}); err != nil {
		util.LogError("InitMySQL err: %v", err)
		return err
	}
	DB = db
	return nil
}

func CloseMySQL() error {
	if DB == nil {
		return nil
	}
	sqlDB, err := DB.DB()
	if err != nil {
		util.LogError("CloseMySQL err: %v", err)
		return err
	}
	return sqlDB.Close()
}
