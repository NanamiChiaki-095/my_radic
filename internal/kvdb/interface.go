package kvdb

// KVStore 定义了底层 KV 存储引擎的统一接口
type KVStore interface {
	Open() error
	Close() error
	Set(key, value []byte) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte) error
	BatchSet(keys, values [][]byte) error
	BatchGet(keys [][]byte) ([][]byte, error)
	BatchDelete(keys [][]byte) error
	IterDB(fn func(key, value []byte) error) error
}