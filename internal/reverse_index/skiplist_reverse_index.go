package reverseindex

import (
	"math"
	"my_radic/internal/kvdb"
	"my_radic/types"
	"my_radic/util"
	"runtime"
	"sync"

	"github.com/huandu/skiplist"
	farm "github.com/leemcloughlin/gofarmhash"
)

// IReverseIndex 倒排索引接口定义
type IReverseIndex interface {
	// Add 添加文档到倒排索引
	// keyword: 关键词
	// docId: 文档内部ID
	// value: 倒排表中存储的详细信息（包含特征位等）
	Add(keyword *types.Keyword, docId uint64, value *types.SkipListValue) error

	// Search 基础搜索，返回包含该关键词的文档ID列表
	Search(keyword *types.Keyword) []uint64

	// Delete 从倒排索引中删除指定文档
	Delete(keyword *types.Keyword, docId uint64) error

	// Intersection 计算多个倒排链的交集 (AND操作)
	Intersection(lists ...*skiplist.SkipList) *skiplist.SkipList

	// Union 计算多个倒排链的并集 (OR操作)
	Union(lists ...*skiplist.SkipList) *skiplist.SkipList

	// SearchQuery 执行复杂的布尔查询 (AND/OR/FILTER)
	SearchQuery(query *types.TermQuery) *skiplist.SkipList

	// GetSuggestions 获取搜索建议 (Trie树能力)
	GetSuggestions(prefix string) []string

	IncreaseDocCount()
	DecreaseDocCount()
	GetDocNum() uint64
}

// SkipListReverseIndex 基于跳表的倒排索引实现
// 使用 ConcurrentHashMap 分段存储，降低锁竞争
type SkipListReverseIndex struct {
	table        *util.ConcurrentHashMap // 存储 KeywordString -> SkipList 的映射
	locks        []sync.RWMutex          // 分段锁，用于保护具体的跳表操作
	termTrie     *TermTrie               // Trie树，用于快速拦截无效词和搜索建议
	docCount     int64
	docCountLock sync.RWMutex
	wal          *kvdb.MmapWAL
	walCh        chan *walTask
	walStop      chan struct{}
	walWg        sync.WaitGroup
	walSync      bool
}

type walTask struct {
	entry *kvdb.WALEntry
	done  chan error
}

// getLock 根据key的哈希值获取对应的分段锁
func (indexer *SkipListReverseIndex) getLock(key string) *sync.RWMutex {
	hash := farm.Hash64WithSeed([]byte(key), 0)
	return &indexer.locks[hash%uint64(len(indexer.locks))]
}

// NewSkipListReverseIndex 创建一个新的倒排索引实例
// docNumEstimate: 预估文档数量，用于初始化Map的大小
func NewSkipListReverseIndex(docNumEstimate int) *SkipListReverseIndex {
	util.LogInfo("Initializing SkipListReverseIndex with estimate doc num: %d", docNumEstimate)
	return &SkipListReverseIndex{
		table:    util.NewConcurrentHashMap(runtime.NumCPU(), docNumEstimate),
		locks:    make([]sync.RWMutex, 1024), // 默认1024个分段锁
		termTrie: NewTermTrie(),
		docCount: 0,
	}
}

func NewSkipListReverseIndexWithWAL(docNumEstimate int, wal *kvdb.MmapWAL, buffer int) *SkipListReverseIndex {
	return NewSkipListReverseIndexWithWALMode(docNumEstimate, wal, buffer, true)
}

func NewSkipListReverseIndexWithWALMode(docNumEstimate int, wal *kvdb.MmapWAL, buffer int, sync bool) *SkipListReverseIndex {
	idx := NewSkipListReverseIndex(docNumEstimate)
	idx.EnableWALWithMode(wal, buffer, sync)
	return idx
}

func (index *SkipListReverseIndex) EnableWAL(wal *kvdb.MmapWAL, buffer int) {
	index.EnableWALWithMode(wal, buffer, true)
}

func (index *SkipListReverseIndex) EnableWALWithMode(wal *kvdb.MmapWAL, buffer int, sync bool) {
	if wal == nil {
		return
	}
	if buffer <= 0 {
		buffer = 1024
	}
	index.wal = wal
	index.walCh = make(chan *walTask, buffer)
	index.walStop = make(chan struct{})
	index.walSync = sync
	index.walWg.Add(1)
	go index.walLoop()
}

func (index *SkipListReverseIndex) CloseWAL() error {
	if index.wal == nil {
		return nil
	}
	if index.walStop != nil {
		close(index.walStop)
	}
	if index.walCh != nil {
		close(index.walCh)
	}
	index.walWg.Wait()
	return index.wal.Close()
}

// Add 添加索引: Keyword -> DocID
func (index *SkipListReverseIndex) Add(keyword *types.Keyword, docId uint64, value *types.SkipListValue) error {
	if keyword == nil || keyword.Word == "" {
		return nil
	}
	field := keyword.Field
	if field == "" {
		field = "content"
	}
	key := (&types.Keyword{Field: field, Word: keyword.Word}).ToString()
	waitWAL, err := index.startWAL(&kvdb.WALEntry{
		ActionType: 0,
		Keyword:    key,
		DocID:      docId,
	})
	if err != nil {
		return err
	}
	lock := index.getLock(key)
	lock.Lock()

	util.LogDebug("Add index: key=%s, docId=%d", key, docId)

	var (
		sl         *skiplist.SkipList
		created    bool
		prevExists bool
		prevVal    interface{}
	)

	if item, ok := index.table.Get(key); !ok {
		// 如果不存在，创建新的跳表
		sl = skiplist.New(skiplist.Uint64)
		sl.Set(docId, value)
		index.table.Set(key, sl)
		// 同步添加到 Trie 树
		index.termTrie.Add(keyword.Word, field)
		created = true
	} else {
		// 如果存在，直接插入或更新
		sl = item.(*skiplist.SkipList)
		if prev := sl.Get(docId); prev != nil {
			prevExists = true
			prevVal = prev.Value
		}
		sl.Set(docId, value)
	}
	err = waitWAL()
	if err != nil {
		if created {
			index.table.Remove(key)
			index.termTrie.Remove(keyword.Word, field)
		} else if sl != nil {
			if prevExists {
				sl.Set(docId, prevVal)
			} else {
				sl.Remove(docId)
				if sl.Len() == 0 {
					index.table.Remove(key)
					index.termTrie.Remove(keyword.Word, field)
				}
			}
		}
	}
	lock.Unlock()
	return err
}

// Search 搜索索引: Keyword -> []DocID
func (index *SkipListReverseIndex) Search(keyword *types.Keyword) []uint64 {
	if keyword == nil || keyword.Word == "" {
		return nil
	}
	field := keyword.Field
	if field == "" {
		field = "content"
	}
	// 先通过 Trie 树快速判断是否存在
	if !index.termTrie.Find(keyword.Word, field) {
		return nil
	}

	key := (&types.Keyword{Field: field, Word: keyword.Word}).ToString()
	lock := index.getLock(key)
	lock.RLock()
	defer lock.RUnlock()

	util.LogDebug("Search index: key=%s", key)

	docs := make([]uint64, 0)

	if item, ok := index.table.Get(key); ok {
		sl := item.(*skiplist.SkipList)
		for e := sl.Front(); e != nil; e = e.Next() {
			docs = append(docs, e.Key().(uint64))
		}
	} else {
		return nil
	}
	return docs
}

// Delete 删除索引: Keyword -> DocID
func (index *SkipListReverseIndex) Delete(keyword *types.Keyword, docId uint64) error {
	if keyword == nil || keyword.Word == "" {
		return nil
	}
	field := keyword.Field
	if field == "" {
		field = "content"
	}
	key := (&types.Keyword{Field: field, Word: keyword.Word}).ToString()
	waitWAL, err := index.startWAL(&kvdb.WALEntry{
		ActionType: 1,
		Keyword:    key,
		DocID:      docId,
	})
	if err != nil {
		return err
	}
	lock := index.getLock(key)
	lock.Lock()

	util.LogDebug("Delete index: key=%s, docId=%d", key, docId)

	var (
		sl             *skiplist.SkipList
		prevExists     bool
		prevVal        interface{}
		removedToEmpty bool
	)
	if item, ok := index.table.Get(key); ok {
		sl = item.(*skiplist.SkipList)
		if prev := sl.Get(docId); prev != nil {
			prevExists = true
			prevVal = prev.Value
			sl.Remove(docId)
			// 如果跳表为空，可以考虑移除key以节省内存，但需注意并发安全
			if sl.Len() == 0 {
				index.table.Remove(key)
				// 同步从 Trie 树移除该字段标记
				index.termTrie.Remove(keyword.Word, field)
				removedToEmpty = true
			}
		}
	}
	err = waitWAL()
	if err != nil && prevExists {
		if removedToEmpty {
			index.table.Set(key, sl)
			index.termTrie.Add(keyword.Word, field)
		}
		if sl != nil {
			sl.Set(docId, prevVal)
		}
	}
	lock.Unlock()
	return err
}

// Intersection 求多个跳表的交集 (AND Query)
// 优化算法：利用跳表的有序性和 Find 快速跳转能力，时间复杂度优于 O(N*M)
func (index *SkipListReverseIndex) Intersection(lists ...*skiplist.SkipList) *skiplist.SkipList {
	util.LogDebug("Intersection: starting for %d lists", len(lists))

	if len(lists) == 0 {
		return skiplist.New(skiplist.Uint64)
	}
	if len(lists) == 1 {
		return lists[0]
	}

	result := skiplist.New(skiplist.Uint64)
	cursors := make([]*skiplist.Element, len(lists))

	// 初始化游标
	for i, list := range lists {
		if list == nil || list.Len() == 0 {
			return result // 只要有一个为空，交集即为空
		}
		cursors[i] = list.Front()
	}

	// 循环查找交集
	for {
		var currentMax uint64 = 0
		// 1. 找到当前所有游标指向的值中的最大值
		for _, c := range cursors {
			if c == nil {
				return result // 任意游标耗尽，结束
			}
			val := c.Key().(uint64)
			if val > currentMax {
				currentMax = val
			}
		}

		// 2. 检查是否所有游标都等于 currentMax
		allMatch := true
		for i, c := range cursors {
			val := c.Key().(uint64)
			if val == currentMax {
				continue
			}
			allMatch = false
			// 3. 如果不相等，利用跳表快速跳转到 currentMax (或大于它的第一个位置)
			// 这是跳表求交集的关键优化
			newPos := lists[i].Find(currentMax)
			if newPos == nil {
				return result
			}
			cursors[i] = newPos
		}

		// 4. 如果全匹配，加入结果集，并所有游标后移
		if allMatch {
			var totalScore float32 = 0
			baseVal := cursors[0].Value.(*types.SkipListValue)

			mergedVal := &types.SkipListValue{Id: baseVal.Id, BitsFeature: baseVal.BitsFeature, Score: totalScore}

			for i := range cursors {
				if val, ok := cursors[i].Value.(*types.SkipListValue); ok {
					totalScore += val.Score
				}
				cursors[i] = cursors[i].Next()
			}
			mergedVal.Score = totalScore
			result.Set(currentMax, mergedVal)
		}
	}
}

// Union 求多个跳表的并集 (OR Query)
// 类似多路归并排序的思想
func (index *SkipListReverseIndex) Union(lists ...*skiplist.SkipList) *skiplist.SkipList {
	util.LogDebug("Union: starting for %d lists", len(lists))

	if len(lists) == 0 {
		return skiplist.New(skiplist.Uint64)
	}
	result := skiplist.New(skiplist.Uint64)
	cursors := make([]*skiplist.Element, len(lists))

	// 初始化游标
	for i, list := range lists {
		if list != nil && list.Len() > 0 {
			cursors[i] = list.Front()
		}
	}

	for {
		var currentMin uint64 = 0xFFFFFFFFFFFFFFFF
		foundAny := false

		// 1. 找出所有游标中的最小值
		for _, c := range cursors {
			if c != nil {
				val := c.Key().(uint64)
				if val < currentMin {
					currentMin = val
					foundAny = true
				}
			}
		}
		if !foundAny {
			break // 所有游标都耗尽
		}

		var totalScore float32 = 0
		var baseVal *types.SkipListValue

		for _, c := range cursors {
			if c != nil && c.Key().(uint64) == currentMin {
				if val, ok := c.Value.(*types.SkipListValue); ok {
					if baseVal == nil {
						baseVal = val
					}
					totalScore += val.Score
				}
			}
		}

		// 2. 加入结果集
		if baseVal != nil {
			mergedVal := &types.SkipListValue{Id: baseVal.Id, BitsFeature: baseVal.BitsFeature, Score: totalScore}
			result.Set(currentMin, mergedVal)
		}

		// 3. 所有等于 currentMin 的游标都后移
		for i, c := range cursors {
			if c != nil && c.Key().(uint64) == currentMin {
				cursors[i] = c.Next()
			}
		}
	}
	return result
}

// SearchQuery 递归搜索，支持 Keyword / Must(AND) / Should(OR)
// 这是一个递归过程，构建查询树
func (index *SkipListReverseIndex) SearchQuery(query *types.TermQuery) *skiplist.SkipList {
	var result *skiplist.SkipList

	// 1. 叶子节点：基础 Keyword 查询
	if query.Keyword != nil {
		field := query.Keyword.Field
		if field == "" {
			field = "content"
		}
		// 先通过 Trie 树快速拦截不存在的词项或字段不匹配的情况
		if !index.termTrie.Find(query.Keyword.Word, field) {
			util.LogDebug("SearchQuery Trie Reject: %s in %s", query.Keyword.Word, field)
			return skiplist.New(skiplist.Uint64)
		}

		key := (&types.Keyword{Field: field, Word: query.Keyword.Word}).ToString()
		lock := index.getLock(key)

		lock.RLock()
		defer lock.RUnlock()

		util.LogDebug("SearchQuery leaf: key=%s, onFlag=%b, offFlag=%b", key, query.OnFlag, query.OffFlag)

		result = skiplist.New(skiplist.Uint64)

		if item, ok := index.table.Get(key); ok {
			list := item.(*skiplist.SkipList)

			N := float64(index.GetDocNum())
			if N == 0 {
				N = 1
			}
			DF := float64(list.Len())

			idf := float32(math.Log(N/(DF+1)) + 1.0)
			// 遍历链表，应用位图过滤
			for e := list.Front(); e != nil; e = e.Next() {
				// 获取 Value 准备过滤
				if val, ok := e.Value.(*types.SkipListValue); ok {
					// 只有满足位图过滤条件的才存入结果
					if filterByBits(val.BitsFeature, query.OnFlag, query.OffFlag, query.OrFlag) {
						newVal := &types.SkipListValue{
							Id:          val.Id,
							BitsFeature: val.BitsFeature,
							Score:       val.Score * idf, // TF * IDF
						}
						result.Set(e.Key(), newVal)
					}
				}
			}
		}
	} else if len(query.Must) > 0 {
		// 2. 复合查询：Must (AND)
		util.LogDebug("SearchQuery Must: %d subqueries", len(query.Must))
		results := make([]*skiplist.SkipList, 0, len(query.Must))
		for _, subQ := range query.Must {
			res := index.SearchQuery(subQ)
			results = append(results, res)
		}
		result = index.Intersection(results...)
	} else if len(query.Should) > 0 {
		// 3. 复合查询：Should (OR)
		util.LogDebug("SearchQuery Should: %d subqueries", len(query.Should))
		results := make([]*skiplist.SkipList, 0, len(query.Should))
		for _, subQ := range query.Should {
			res := index.SearchQuery(subQ)
			results = append(results, res)
		}
		result = index.Union(results...)
	} else {
		result = skiplist.New(skiplist.Uint64)
	}

	// 4. 后置过滤 (Post-Filtering)
	// 如果是复合节点 (Keyword == nil)，但却携带了过滤条件，则需要对结果集再次过滤。
	// 这允许用户在 AND/OR 节点上通过 OnFlag/OffFlag 对子树结果进行二次筛选。
	if query.Keyword == nil && result != nil && result.Len() > 0 {
		// 只要有任何一个 flag 有值，就说明需要后置过滤
		if query.OnFlag > 0 || query.OffFlag > 0 || len(query.OrFlag) > 0 {
			util.LogDebug("Post-Filtering result set of size %d", result.Len())
			filteredResult := skiplist.New(skiplist.Uint64)
			for e := result.Front(); e != nil; e = e.Next() {
				if val, ok := e.Value.(*types.SkipListValue); ok {
					if filterByBits(val.BitsFeature, query.OnFlag, query.OffFlag, query.OrFlag) {
						filteredResult.Set(e.Key(), val)
					}
				}
			}
			result = filteredResult
		}
	}

	return result
}

// GetSuggestions 获取全局搜索提示
func (index *SkipListReverseIndex) GetSuggestions(prefix string) []string {
	return index.termTrie.GlobalSuggestion(prefix)
}

// filterByBits 根据位图特征进行过滤
// bits: 文档的特征值
// onFlag: 必须全为1的位
// offFlag: 必须全为0的位
// orFlags: 只要有一个匹配即可
func filterByBits(bits, onFlag, offFlag uint64, orFlags []uint64) bool {
	// 必须包含 onFlag 的所有位
	if onFlag > 0 && bits&onFlag != onFlag {
		return false
	}
	// 不能包含 offFlag 的任何一位
	if offFlag > 0 && bits&offFlag != 0 {
		return false
	}
	// 必须包含 orFlags 列表中的至少一个
	if len(orFlags) > 0 {
		orMatched := false
		for _, flag := range orFlags {
			if flag > 0 && bits&flag != 0 {
				orMatched = true
				break
			}
		}
		if !orMatched {
			return false
		}
	}
	return true
}

func (index *SkipListReverseIndex) IncreaseDocCount() {
	index.docCountLock.Lock()
	defer index.docCountLock.Unlock()
	index.docCount++
}

func (index *SkipListReverseIndex) DecreaseDocCount() {
	index.docCountLock.Lock()
	defer index.docCountLock.Unlock()
	if index.docCount > 0 {
		index.docCount--
	}
}

func (index *SkipListReverseIndex) GetDocNum() uint64 {
	index.docCountLock.RLock()
	defer index.docCountLock.RUnlock()
	return uint64(index.docCount)
}

func (index *SkipListReverseIndex) walLoop() {
	defer index.walWg.Done()
	for {
		select {
		case task, ok := <-index.walCh:
			if !ok {
				return
			}
			if task == nil {
				continue
			}
			var err error
			if task.entry != nil {
				_, err = index.wal.Append(task.entry)
				if err == nil && task.done != nil {
					err = index.wal.Flush()
				}
			}
			if task.done != nil {
				task.done <- err
				close(task.done)
			}
		case <-index.walStop:
			return
		}
	}
}

func (index *SkipListReverseIndex) startWAL(entry *kvdb.WALEntry) (func() error, error) {
	if index.wal == nil || entry == nil {
		return func() error { return nil }, nil
	}
	if index.walCh == nil {
		return func() error {
			if _, err := index.wal.Append(entry); err != nil {
				return err
			}
			if index.walSync {
				return index.wal.Flush()
			}
			return nil
		}, nil
	}
	var done chan error
	if index.walSync {
		done = make(chan error, 1)
	}
	task := &walTask{entry: entry, done: done}
	select {
	case index.walCh <- task:
		if task.done == nil {
			return func() error { return nil }, nil
		}
		return func() error { return <-task.done }, nil
	default:
		return func() error {
			if _, err := index.wal.Append(entry); err != nil {
				return err
			}
			if index.walSync {
				return index.wal.Flush()
			}
			return nil
		}, nil
	}
}
