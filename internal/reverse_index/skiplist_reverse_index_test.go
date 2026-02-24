package reverseindex

import (
	"fmt"
	"my_radic/types"
	"sync"
	"testing"

	"github.com/huandu/skiplist"
	"github.com/stretchr/testify/assert"
)

func TestSkipListReverseIndex_Basic(t *testing.T) {
	// 初始化，预估 100 个文档
	idx := NewSkipListReverseIndex(100)
	kw := &types.Keyword{Field: "title", Word: "golang"}

	// 1. 测试添加与去重
	assert.NoError(t, idx.Add(kw, 1, &types.SkipListValue{Id: "1", BitsFeature: 0}))
	assert.NoError(t, idx.Add(kw, 2, &types.SkipListValue{Id: "2", BitsFeature: 0}))
	assert.NoError(t, idx.Add(kw, 1, &types.SkipListValue{Id: "1", BitsFeature: 0}))

	res := idx.Search(kw)
	assert.Equal(t, 2, len(res), "应该有两个文档ID")
	assert.Equal(t, uint64(1), res[0], "结果应该有序")
	assert.Equal(t, uint64(2), res[1])

	// 2. 测试删除
	assert.NoError(t, idx.Delete(kw, 1))
	res2 := idx.Search(kw)
	assert.Equal(t, 1, len(res2))
	assert.NotContains(t, res2, uint64(1))

	// 3. 测试删除最后一个元素后的清理
	assert.NoError(t, idx.Delete(kw, 2))
	res3 := idx.Search(kw)
	assert.Nil(t, res3, "没有任何文档时应返回 nil")
}

func TestSkipListReverseIndex_Intersection(t *testing.T) {
	idx := NewSkipListReverseIndex(100)

	kw1 := &types.Keyword{Field: "title", Word: "gold"}
	kw2 := &types.Keyword{Field: "title", Word: "silver"}

	// kw1: [1, 3, 5, 7]
	assert.NoError(t, idx.Add(kw1, 1, &types.SkipListValue{}))
	assert.NoError(t, idx.Add(kw1, 3, &types.SkipListValue{}))
	assert.NoError(t, idx.Add(kw1, 5, &types.SkipListValue{}))
	assert.NoError(t, idx.Add(kw1, 7, &types.SkipListValue{}))

	// kw2: [2, 3, 6, 7]
	assert.NoError(t, idx.Add(kw2, 2, &types.SkipListValue{}))
	assert.NoError(t, idx.Add(kw2, 3, &types.SkipListValue{}))
	assert.NoError(t, idx.Add(kw2, 6, &types.SkipListValue{}))
	assert.NoError(t, idx.Add(kw2, 7, &types.SkipListValue{}))

	// 获取内部跳表进行测试 (由于 Intersection 接收 *skiplist.SkipList)
	// 在实际 indexer 中，我们会通过内部方法获取
	item1, _ := idx.table.Get(kw1.ToString())
	item2, _ := idx.table.Get(kw2.ToString())

	sl1 := item1.(*skiplist.SkipList)
	sl2 := item2.(*skiplist.SkipList)

	resSl := idx.Intersection(sl1, sl2)

	// 验证交集结果: [3, 7]
	assert.Equal(t, 2, resSl.Len())
	assert.NotNil(t, resSl.Get(uint64(3)))
	assert.NotNil(t, resSl.Get(uint64(7)))
}

func TestSkipListReverseIndex_Union(t *testing.T) {
	idx := NewSkipListReverseIndex(100)

	kw1 := &types.Keyword{Field: "title", Word: "A"}
	kw2 := &types.Keyword{Field: "title", Word: "B"}

	// kw1: [1, 2]
	assert.NoError(t, idx.Add(kw1, 1, &types.SkipListValue{}))
	assert.NoError(t, idx.Add(kw1, 2, &types.SkipListValue{}))

	// kw2: [2, 3]
	assert.NoError(t, idx.Add(kw2, 2, &types.SkipListValue{}))
	assert.NoError(t, idx.Add(kw2, 3, &types.SkipListValue{}))

	item1, _ := idx.table.Get(kw1.ToString())
	item2, _ := idx.table.Get(kw2.ToString())

	sl1 := item1.(*skiplist.SkipList)
	sl2 := item2.(*skiplist.SkipList)

	resSl := idx.Union(sl1, sl2)

	// 验证并集结果: [1, 2, 3]
	assert.Equal(t, 3, resSl.Len())
	assert.NotNil(t, resSl.Get(uint64(1)))
	assert.NotNil(t, resSl.Get(uint64(2)))
	assert.NotNil(t, resSl.Get(uint64(3)))
}

func TestSkipListReverseIndex_Concurrency(t *testing.T) {
	idx := NewSkipListReverseIndex(1000)
	kw := &types.Keyword{Field: "title", Word: "concurrent"}

	var wg sync.WaitGroup
	numRoutines := 100
	docsPerRoutine := 50

	// 并发写入
	for i := 0; i < numRoutines; i++ {
		wg.Add(1)
		go func(routineID int) {
			defer wg.Done()
			for j := 0; j < docsPerRoutine; j++ {
				docID := uint64(routineID*docsPerRoutine + j)
				_ = idx.Add(kw, docID, &types.SkipListValue{})
			}
		}(i)
	}

	wg.Wait()

	// 验证总数
	res := idx.Search(kw)
	assert.Equal(t, numRoutines*docsPerRoutine, len(res))

	// 并发删除一半
	for i := 0; i < numRoutines/2; i++ {
		wg.Add(1)
		go func(routineID int) {
			defer wg.Done()
			for j := 0; j < docsPerRoutine; j++ {
				docID := uint64(routineID*docsPerRoutine + j)
				_ = idx.Delete(kw, docID)
			}
		}(i)
	}

	wg.Wait()

	res2 := idx.Search(kw)
	assert.Equal(t, (numRoutines-numRoutines/2)*docsPerRoutine, len(res2))
}

func TestSkipListReverseIndex_MixedConcurrency(t *testing.T) {
	idx := NewSkipListReverseIndex(1000)

	var wg sync.WaitGroup
	// 模拟多个不同关键词的并发操作
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			kw := &types.Keyword{Field: "title", Word: fmt.Sprintf("word-%d", i%10)} // 10个词竞争
			_ = idx.Add(kw, uint64(i), &types.SkipListValue{})
			idx.Search(kw)
			if i%2 == 0 {
				_ = idx.Delete(kw, uint64(i))
			}
		}(i)
	}
	wg.Wait()
}

func TestSkipListReverseIndex_SearchQuery_Filter(t *testing.T) {
	idx := NewSkipListReverseIndex(100)
	kw := &types.Keyword{Field: "tag", Word: "news"}

	// Feature Bits:
	// Doc 1: 001 (1)
	// Doc 2: 010 (2)
	// Doc 3: 011 (3)
	// Doc 4: 100 (4)

	assert.NoError(t, idx.Add(kw, 1, &types.SkipListValue{BitsFeature: 1}))
	assert.NoError(t, idx.Add(kw, 2, &types.SkipListValue{BitsFeature: 2}))
	assert.NoError(t, idx.Add(kw, 3, &types.SkipListValue{BitsFeature: 3}))
	assert.NoError(t, idx.Add(kw, 4, &types.SkipListValue{BitsFeature: 4}))

	// Test 1: OnFlag = 1 (Must have bit 1) -> Doc 1 (001), Doc 3 (011)
	q1 := &types.TermQuery{
		Keyword: kw,
		OnFlag:  1,
	}
	res1 := idx.SearchQuery(q1)
	assert.Equal(t, 2, res1.Len())
	assert.NotNil(t, res1.Get(uint64(1)))
	assert.NotNil(t, res1.Get(uint64(3)))

	// Test 2: OffFlag = 2 (Must NOT have bit 2) -> Doc 1 (001), Doc 4 (100)
	// Doc 2 (010) - Fail
	// Doc 3 (011) - Fail
	q2 := &types.TermQuery{
		Keyword: kw,
		OffFlag: 2,
	}
	res2 := idx.SearchQuery(q2)
	assert.Equal(t, 2, res2.Len())
	assert.NotNil(t, res2.Get(uint64(1)))
	assert.NotNil(t, res2.Get(uint64(4)))

	// Test 3: OrFlag = [4] (Must have bit 4) -> Doc 4
	q3 := &types.TermQuery{
		Keyword: kw,
		OrFlag:  []uint64{4},
	}
	res3 := idx.SearchQuery(q3)
	assert.Equal(t, 1, res3.Len())
	assert.NotNil(t, res3.Get(uint64(4)))

	// Test 4: OnFlag=1 AND OffFlag=2 -> Doc 1 (001)
	// Doc 3 (011) fails OffFlag=2
	q4 := &types.TermQuery{
		Keyword: kw,
		OnFlag:  1,
		OffFlag: 2,
	}
	res4 := idx.SearchQuery(q4)
	assert.Equal(t, 1, res4.Len())
	assert.NotNil(t, res4.Get(uint64(1)))
}
