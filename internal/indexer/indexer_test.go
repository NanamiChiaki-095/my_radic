package indexer

import (
	"my_radic/internal/kvdb"
	reverseindex "my_radic/internal/reverse_index"
	"my_radic/types"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIndexer_Integration(t *testing.T) {
	// 1. 准备环境 (BoltDB 文件路径)
	dbPath := "test_integration.db"
	defer os.Remove(dbPath) // 测试完删库

	// 2. 初始化组件
	forward := kvdb.NewBolt().WithDataPath(dbPath).WithBucket("docs")
	err := forward.Open()
	assert.NoError(t, err)
	defer forward.Close()

	// 初始化倒排索引，预估 1000 个文档
	reverse := reverseindex.NewSkipListReverseIndex(1000)
	idx := NewIndexer(forward, reverse)

	// 3. 准备测试数据 (模拟四篇带有不同关键词的文档)
	docs := []*types.Document{
		{IntId: 1, Keyword: []*types.Keyword{{Field: "content", Word: "golang"}, {Field: "content", Word: "tutorial"}}}, // Doc 1: golang, tutorial
		{IntId: 2, Keyword: []*types.Keyword{{Field: "content", Word: "golang"}, {Field: "content", Word: "search"}, {Field: "content", Word: "engine"}}}, // Doc 2: golang, search, engine
		{IntId: 3, Keyword: []*types.Keyword{{Field: "content", Word: "search"}, {Field: "content", Word: "engine"}}}, // Doc 3: search, engine
		{IntId: 4, Keyword: []*types.Keyword{{Field: "content", Word: "python"}, {Field: "content", Word: "tutorial"}}}, // Doc 4: python, tutorial
	}

	// 4. 批量灌入索引
	ids, err := idx.BatchAddDocument(docs)
	assert.NoError(t, err)
	assert.Equal(t, 4, len(ids))

	// --- 开始复杂查询测试 ---

	// Case 1: 简单关键词查询 "golang"
	// 预期结果: Doc 1, Doc 2
	t.Run("Search Keyword: golang", func(t *testing.T) {
		q := &types.TermQuery{
			Keyword: &types.Keyword{Word: "golang"},
		}
		res, err := idx.SearchComplex(q)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(res))
		checkDocIds(t, res, 1, 2)
	})

	// Case 2: AND 查询 (Must) -> "golang" AND "engine"
	// 预期结果: 只有 Doc 2 包含这两个词
	t.Run("Search AND: golang + engine", func(t *testing.T) {
		q := &types.TermQuery{
			Must: []*types.TermQuery{
				{Keyword: &types.Keyword{Word: "golang"}},
				{Keyword: &types.Keyword{Word: "engine"}},
			},
		}
		res, err := idx.SearchComplex(q)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(res))
		checkDocIds(t, res, 2)
	})

	// Case 3: OR 查询 (Should) -> "golang" OR "python"
	// 预期结果: Doc 1, 2 (golang) 和 Doc 4 (python)
	t.Run("Search OR: golang | python", func(t *testing.T) {
		q := &types.TermQuery{
			Should: []*types.TermQuery{
				{Keyword: &types.Keyword{Word: "golang"}},
				{Keyword: &types.Keyword{Word: "python"}},
			},
		}
		res, err := idx.SearchComplex(q)
		assert.NoError(t, err)
		assert.Equal(t, 3, len(res))
		checkDocIds(t, res, 1, 2, 4)
	})

	// Case 4: 嵌套复杂查询 -> (golang AND tutorial) OR search
	// 预期结果:
	// - (golang AND tutorial) -> Doc 1
	// - search -> Doc 2, Doc 3
	// 并集结果 -> [1, 2, 3]
	t.Run("Search Complex: (golang & tutorial) | search", func(t *testing.T) {
		subAnd := &types.TermQuery{
			Must: []*types.TermQuery{
				{Keyword: &types.Keyword{Word: "golang"}},
				{Keyword: &types.Keyword{Word: "tutorial"}},
			},
		}
		q := &types.TermQuery{
			Should: []*types.TermQuery{
				subAnd,
				{Keyword: &types.Keyword{Word: "search"}},
			},
		}
		res, err := idx.SearchComplex(q)
		assert.NoError(t, err)
		assert.Equal(t, 3, len(res))
		checkDocIds(t, res, 1, 2, 3)
	})
}

// checkDocIds 辅助函数: 验证结果中的 IntId 是否匹配 (不计较顺序)
func checkDocIds(t *testing.T, docs []*types.Document, expectedIds ...uint64) {
	docMap := make(map[uint64]bool)
	for _, d := range docs {
		docMap[d.IntId] = true
	}
	for _, id := range expectedIds {
		assert.True(t, docMap[id], "Expected DocID %d not found in results", id)
	}
}

func TestIndexer_TFIDF_And_Recovery(t *testing.T) {
	dbPath := "test_tfidf_recovery.db"
	os.RemoveAll(dbPath) // 清理旧数据
	defer os.RemoveAll(dbPath)

	// --- 阶段 1: 启动 Indexer，写入数据，验证打分 ---
	forward := kvdb.NewBolt().WithDataPath(dbPath).WithBucket("docs")
	forward.Open()
	reverse := reverseindex.NewSkipListReverseIndex(1000)
	idx := NewIndexer(forward, reverse)

	// 模拟数据：
	// "common": 出现 3 次 (DF=3) -> IDF 低
	// "rare": 出现 1 次 (DF=1) -> IDF 高
	docs := []*types.Document{
		{IntId: 1, Id: "doc1", Keyword: []*types.Keyword{{Word: "common"}, {Word: "apple"}}},
		{IntId: 2, Id: "doc2", Keyword: []*types.Keyword{{Word: "common"}, {Word: "banana"}}},
		{IntId: 3, Id: "doc3", Keyword: []*types.Keyword{{Word: "common"}, {Word: "rare"}}},
	}
	idx.BatchAddDocument(docs)

	// 验证 "common" 的分数
	t.Run("Score Check", func(t *testing.T) {
		resCommon, _ := idx.SearchComplex(&types.TermQuery{Keyword: &types.Keyword{Word: "common"}})
		assert.Equal(t, 3, len(resCommon))
		scoreCommon := resCommon[0].Score

		resRare, _ := idx.SearchComplex(&types.TermQuery{Keyword: &types.Keyword{Word: "rare"}})
		assert.Equal(t, 1, len(resRare))
		scoreRare := resRare[0].Score

		t.Logf("Score 'common' (DF=3): %f", scoreCommon)
		t.Logf("Score 'rare'   (DF=1): %f", scoreRare)

		assert.True(t, scoreRare > scoreCommon, "Rare word should have higher score than common word")
	})

	// 关闭索引（模拟停机）
	idx.Close()

	// --- 阶段 2: 重启 Indexer，从 BoltDB 恢复，验证一致性 ---
	t.Run("Recovery Check", func(t *testing.T) {
		// 重新打开 BoltDB
		newForward := kvdb.NewBolt().WithDataPath(dbPath).WithBucket("docs")
		newForward.Open()
		// 全新的倒排索引（内存是空的）
		newReverse := reverseindex.NewSkipListReverseIndex(1000)
		newIdx := NewIndexer(newForward, newReverse)

		// 核心：执行恢复逻辑
		count, err := newIdx.LoadFromForwardIndex()
		assert.NoError(t, err)
		assert.Equal(t, 3, count, "Should recover 3 documents")

		// 再次搜索 "rare"，检查分数是否和之前一致
		res, _ := newIdx.SearchComplex(&types.TermQuery{Keyword: &types.Keyword{Word: "rare"}})
		assert.Equal(t, 1, len(res))
		
		t.Logf("Recovered Score 'rare': %f", res[0].Score)
		
		// 简单验证：只要分数依然很高（说明 N 和 DF 恢复了），且能搜出来，就说明成功
		assert.True(t, res[0].Score > 1.0) 

		newIdx.Close()
	})
}

func TestIndexer_AutoTokenizer(t *testing.T) {
	dbPath := "test_tokenizer.db"
	os.RemoveAll(dbPath)
	defer os.RemoveAll(dbPath)

	forward := kvdb.NewBolt().WithDataPath(dbPath).WithBucket("docs")
	forward.Open()
	reverse := reverseindex.NewSkipListReverseIndex(1000)
	idx := NewIndexer(forward, reverse)
	defer idx.Close()

	// 1. 添加一个只有 Content，没有 Keyword 的文档
	doc := &types.Document{
		IntId:   1,
		Id:      "doc_auto",
		Content: "我爱北京天安门", // 这是一个经典的中文分词测试句
	}
	_, err := idx.AddDocument(doc)
	assert.NoError(t, err)

	// 2. 搜索 "北京"
	t.Run("Search '北京'", func(t *testing.T) {
		q := &types.TermQuery{
			Keyword: &types.Keyword{Field: "content", Word: "北京"},
		}
		res, err := idx.SearchComplex(q)
		assert.NoError(t, err)
		if assert.Equal(t, 1, len(res)) {
			assert.Equal(t, "doc_auto", res[0].Id)
		}
	})

	// 3. 搜索 "天安门"
	t.Run("Search '天安门'", func(t *testing.T) {
		q := &types.TermQuery{
			Keyword: &types.Keyword{Field: "content", Word: "天安门"},
		}
		res, err := idx.SearchComplex(q)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(res))
	})

	// 4. 验证 Document 对象里的 Keyword 字段是否被回填了
	t.Run("Check Keywords Population", func(t *testing.T) {
		assert.True(t, len(doc.Keyword) > 0, "Keywords should be populated")
		found := false
		for _, k := range doc.Keyword {
			if k.Word == "北京" {
				found = true
				break
			}
		}
		assert.True(t, found, "Keyword '北京' should exist in document keywords")
	})
}