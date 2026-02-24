package kvdb

import (
	"errors"
	"my_radic/util"
	"os"

	"github.com/dgraph-io/badger/v4"
)

// Badger 封装了 BadgerDB 实例
type Badger struct {
	db       *badger.DB
	dataPath string
}

// NewBadger 创建一个新的 Badger 实例
func NewBadger() *Badger {
	return &Badger{}
}

// WithDataPath 设置数据库文件存储路径
func (b *Badger) WithDataPath(path string) *Badger {
	b.dataPath = path
	return b
}

// Open 打开数据库
// 如果目录不存在会自动创建
func (b *Badger) Open() error {
	if err := os.MkdirAll(b.dataPath, 0755); err != nil {
		return err
	}
	// 使用默认配置
	opt := badger.DefaultOptions(b.dataPath)
	// 关闭 Badger 默认的 Logger 输出，避免日志刷屏
	opt.Logger = nil
	
	db, err := badger.Open(opt)
	if err != nil {
		return err
	}
	b.db = db
	util.LogInfo("BadgerDB opened at %s", b.dataPath)
	return nil
}

// Close 关闭数据库
func (b *Badger) Close() error {
	if b.db == nil {
		return nil
	}
	return b.db.Close()
}

// Set 写入单个 Key-Value
func (b *Badger) Set(key, value []byte) error {
	if b.db == nil {
		util.LogError("BadgerDB not opened")
		return errors.New("BadgerDB not opened")
	}
	// 使用 Update 事务进行写操作
	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

// Get 读取单个 Key
func (b *Badger) Get(key []byte) ([]byte, error) {
	if b.db == nil {
		util.LogError("BadgerDB not opened")
		return nil, errors.New("BadgerDB not opened")
	}
	var value []byte
	// 使用 View 事务进行读操作
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			// 如果 key 不存在，这里会返回 badger.ErrKeyNotFound
			// 调用者需要处理这个错误
			return err
		}
		// ValueCopy 也是必须的，因为 item.Value() 返回的切片在事务结束后会失效
		value, err = item.ValueCopy(nil)
		return err
	})
	if err != nil {
		return nil, err
	}
	return value, nil
}

// Delete 删除单个 Key
func (b *Badger) Delete(key []byte) error {
	if b.db == nil {
		util.LogError("BadgerDB not opened")
		return errors.New("BadgerDB not opened")
	}
	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

// BatchSet 批量写入
// 使用 WriteBatch 进行高性能批量写入，适合大量数据导入场景
func (b *Badger) BatchSet(keys, value [][]byte) error {
	if b.db == nil {
		return errors.New("BadgerDB not opened")
	}
	
	wb := b.db.NewWriteBatch()
	defer wb.Cancel()

	for i, key := range keys {
		if err := wb.Set(key, value[i]); err != nil {
			util.LogError("BadgerDB BatchSet key %s error: %v", key, err)
			return err
		}
	}
	// 必须调用 Flush 确保数据落盘
	if err := wb.Flush(); err != nil {
		util.LogError("BadgerDB BatchSet Flush error: %v", err)
		return err
	}
	return nil
}

// BatchGet 批量读取
// 如果某个 Key 不存在，对应位置返回 nil，不会中断整个读取过程
func (b *Badger) BatchGet(keys [][]byte) ([][]byte, error) {
	if b.db == nil {
		util.LogError("BadgerDB not opened")
		return nil, errors.New("BadgerDB not opened")
	}
	values := make([][]byte, len(keys))
	
	err := b.db.View(func(txn *badger.Txn) error {
		for i, key := range keys {
			item, err := txn.Get(key)
			if err != nil {
				// 关键修复：如果 key 不存在，忽略该错误，对应位置保持 nil
				if err == badger.ErrKeyNotFound {
					continue
				}
				util.LogError("BadgerDB BatchGet key %s error: %v", key, err)
				return err
			}
			
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			values[i] = val
		}
		return nil
	})
	return values, err
}

// BatchDelete 批量删除
// 同样使用 WriteBatch 优化性能
func (b *Badger) BatchDelete(keys [][]byte) error {
	if b.db == nil {
		util.LogError("BadgerDB not opened")
		return errors.New("BadgerDB not opened")
	}
	wb := b.db.NewWriteBatch()
	defer wb.Cancel()
	
	for _, key := range keys {
		if err := wb.Delete(key); err != nil {
			util.LogError("BadgerDB BatchDelete key %s error: %v", key, err)
			return err
		}
	}
	if err := wb.Flush(); err != nil {
		util.LogError("BadgerDB BatchDelete Flush error: %v", err)
		return err
	}
	return nil
}

// IterDB 遍历数据库
// fn 返回错误时会终止遍历
func (b *Badger) IterDB(fn func(key, value []byte) error) error {
	if b.db == nil {
		return errors.New("BadgerDB not opened")
	}
	return b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		// 开启预取值优化，适合全量扫描 value 的场景
		opts.PrefetchValues = true
		opts.PrefetchSize = 100 // 默认预取 100 条
		
		it := txn.NewIterator(opts)
		defer it.Close()
		
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			// 注意：必须使用 Copy 版本，否则 key/value 指针在 next 后会失效
			key := item.KeyCopy(nil)
			value, err := item.ValueCopy(nil)
			if err != nil {
				util.LogError("BadgerDB IterDB Get key %s error: %v", key, err)
				return err
			}
			if err := fn(key, value); err != nil {
				util.LogError("BadgerDB IterDB fn error: %v", err)
				return err
			}
		}
		return nil
	})
}