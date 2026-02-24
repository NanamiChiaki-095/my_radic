package kvdb

import (
	"errors"

	bolt "go.etcd.io/bbolt"
)

var ErrNoData = errors.New("no data")

// Bolt 实现了 KVStore 接口
type Bolt struct {
	db     *bolt.DB
	path   string // 数据库文件路径
	bucket []byte // Bucket 名称
}

func NewBolt() *Bolt {
	return &Bolt{
		bucket: []byte("doc_store"), // 默认 Bucket
	}
}

// WithDataPath 设置数据库文件路径
func (s *Bolt) WithDataPath(path string) *Bolt {
	s.path = path
	return s
}

// WithBucket 设置要操作的 Bucket
func (s *Bolt) WithBucket(bucket string) *Bolt {
	s.bucket = []byte(bucket)
	return s
}

func (s *Bolt) Open() error {
	db, err := bolt.Open(s.path, 0600, nil)
	if err != nil {
		return err
	}
	// 初始化时确保 Bucket 存在
	err = db.Update((func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(s.bucket)
		return err
	}))
	if err != nil {
		db.Close()
		return err
	}
	s.db = db
	return nil
}

func (s *Bolt) Close() error {
	if s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Bolt) Set(k, v []byte) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(s.bucket).Put(k, v)
	})
}

func (s *Bolt) Get(k []byte) ([]byte, error) {
	var ival []byte
	err := s.db.View(func(tx *bolt.Tx) error {
		val := tx.Bucket(s.bucket).Get(k)
		if val != nil {
			ival = make([]byte, len(val))
			copy(ival, val)
		}
		return nil
	})
	if len(ival) == 0 {
		return nil, ErrNoData
	}
	return ival, err
}

func (s *Bolt) Delete(k []byte) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(s.bucket).Delete(k)
	})
}

func (s *Bolt) BatchSet(keys, values [][]byte) error {
	if len(keys) != len(values) {
		return errors.New("key value length mismatch")
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucket)
		for i, k := range keys {
			if err := b.Put(k, values[i]); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Bolt) BatchGet(keys [][]byte) ([][]byte, error) {
	values := make([][]byte, len(keys))
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucket)
		for i, k := range keys {
			val := b.Get(k)
			if val != nil {
				ival := make([]byte, len(val))
				copy(ival, val)
				values[i] = ival
			}
		}
		return nil
	})
	return values, err
}

func (s *Bolt) BatchDelete(keys [][]byte) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucket)
		for _, k := range keys {
			if err := b.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
}

// IterDB 实现接口签名: IterDB(fn) error
func (s *Bolt) IterDB(fn func(k, v []byte) error) error {
	return s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucket)
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if err := fn(k, v); err != nil {
				return err
			}
		}
		return nil
	})
}
