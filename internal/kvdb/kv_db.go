package kvdb

type IKeyValueDB interface {
	Open() error                              //打开/初始化数据库
	GetDbPath() string                        //获取数据库路径
	Set(k, v []byte) error                    //写入数据
	Get(k []byte) ([]byte, error)             //读取数据
	BatchGet(keys [][]byte) ([][]byte, error) //批量读取数据
	Delete(k []byte) error                    //删除数据
	BatchDelete(keys [][]byte) error          //批量删除数据
	BatchSet(keys, values [][]byte) error     //批量写入数据
	Has(k []byte) bool                        //判断key是否存在
	IterDB(fn func(k, v []byte) error) int64  //遍历数据库
	IterKey(fn func(k []byte) error) int64    //遍历所有key
	Close() error                             //关闭数据库
}
