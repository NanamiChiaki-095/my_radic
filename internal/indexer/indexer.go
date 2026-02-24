package indexer

import (
	"encoding/binary"
	"errors"
	"my_radic/internal/kvdb"
	reverseindex "my_radic/internal/reverse_index"
	"my_radic/types"
	"my_radic/util"
	"strings"

	"google.golang.org/protobuf/proto"
)

// IIndexer 定义了索引器的核心能力标准
type IIndexer interface {
	AddDocument(doc *types.Document) (uint64, error)
	DeleteDocument(docId uint64) error
	GetDocument(docId uint64) (*types.Document, error)
	Search(kw *types.Keyword) ([]*types.Document, error)
	SearchComplex(query *types.TermQuery) ([]*types.Document, error)
	BatchAddDocument(docs []*types.Document) ([]uint64, error)
	BatchDeleteDocument(docIds []uint64) error
	LoadFromForwardIndex() (int, error)
	GetSuggestions(prefix string) []string
	Close() error
}

func (idx *Indexer) addReverseIndex(doc *types.Document) error {
	counts := make(map[kwKey]float32)

	for _, kw := range doc.Keyword {
		if kw != nil {
			counts[kwKey{Field: kw.Field, Word: kw.Word}]++
		}
	}
	for key, count := range counts {
		slv := &types.SkipListValue{Id: doc.Id, BitsFeature: doc.BitsFeature, Score: count}
		if slv != nil {
			if err := idx.reverseIndex.Add(&types.Keyword{Field: key.Field, Word: key.Word}, doc.IntId, slv); err != nil {
				return err
			}
		}
	}
	idx.reverseIndex.IncreaseDocCount()
	return nil
}

// Indexer 索引器具体实现
type Indexer struct {
	forwardIndex kvdb.KVStore
	reverseIndex reverseindex.IReverseIndex
}

// 统计词频
type kwKey struct {
	Field string
	Word  string
}

// NewIndexer 构造函数
func NewIndexer(forward kvdb.KVStore, reverse reverseindex.IReverseIndex) *Indexer {
	return &Indexer{
		forwardIndex: forward,
		reverseIndex: reverse,
	}
}

// AddDocument 添加单个文档
// 核心流程：
// 1. 分词：将 Content 切分为关键词，填充到 Doc.Keyword 字段。
// 2. 序列化：将包含关键词的 Doc 序列化为 Protobuf 二进制。
// 3. 写正排：存入底层 KVStore (BoltDB/BadgerDB)，作为持久化存储。
// 4. 写倒排：更新内存中的跳表倒排索引 (SkipListReverseIndex)，用于检索。
// 注意：必须先分词再序列化，否则重启后从正排恢复时会丢失关键词信息。
func (idx *Indexer) AddDocument(doc *types.Document) (uint64, error) {
	if doc == nil {
		return 0, errors.New("document is nil")
	}

	if doc.IntId == 0 {
		return 0, errors.New("doc.IntId is required")
	}

	// 必须在序列化之前进行分词，这样关键词才能存入正排索引
	tokenizer := util.GetTokenizer()
	if doc.Title != "" {
		words := tokenizer.Cut(doc.Title)
		for _, w := range words {
			doc.Keyword = append(doc.Keyword, &types.Keyword{Field: "title", Word: w})
		}
	}
	if doc.Content != "" {
		words := tokenizer.Cut(doc.Content)
		for _, w := range words {
			doc.Keyword = append(doc.Keyword, &types.Keyword{Field: "content", Word: w})
		}
	}

	docBytes, err := proto.Marshal(doc)
	if err != nil {
		util.LogError("Failed to marshal document %d: %v", doc.IntId, err)
		return 0, err
	}

	keyBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(keyBytes, doc.IntId)

	if err := idx.forwardIndex.Set(keyBytes, docBytes); err != nil {
		util.LogError("Failed to write forward index for doc %d: %v", doc.IntId, err)
		return 0, err
	}

	if err := idx.addReverseIndex(doc); err != nil {
		return 0, err
	}
	return doc.IntId, nil
}

// GetDocument 获取单个文档
func (idx *Indexer) GetDocument(docId uint64) (*types.Document, error) {
	if docId == 0 {
		return nil, errors.New("docId is required")
	}

	keyBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(keyBytes, docId)

	docBytes, err := idx.forwardIndex.Get(keyBytes)
	if err != nil {
		return nil, err
	}

	doc := &types.Document{}
	if err := proto.Unmarshal(docBytes, doc); err != nil {
		util.LogError("Failed to unmarshal document %d: %v", docId, err)
		return nil, err
	}
	return doc, nil
}

// DeleteDocument 删除单个文档
func (idx *Indexer) DeleteDocument(docId uint64) error {
	if docId == 0 {
		return errors.New("docId is required")
	}

	doc, err := idx.GetDocument(docId)
	if err != nil {
		util.LogWarn("DeleteDocument failed: could not find doc %d: %v", docId, err)
		return err
	}

	for _, kw := range doc.Keyword {
		if kw != nil {
			if err := idx.reverseIndex.Delete(kw, docId); err != nil {
				return err
			}
		}
	}

	keyBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(keyBytes, docId)
	if err := idx.forwardIndex.Delete(keyBytes); err != nil {
		util.LogError("Failed to delete from forward index doc %d: %v", docId, err)
		return err
	}
	idx.reverseIndex.DecreaseDocCount()
	return nil
}

// Search 简单搜索接口
func (idx *Indexer) Search(kw *types.Keyword) ([]*types.Document, error) {
	if kw == nil || kw.Field == "" {
		return nil, errors.New("field is required")
	}

	docIds := idx.reverseIndex.Search(kw)
	if len(docIds) == 0 {
		return nil, nil
	}

	docs := make([]*types.Document, 0, len(docIds))
	for _, docId := range docIds {
		doc, err := idx.GetDocument(docId)
		if err != nil {
			continue
		}
		docs = append(docs, doc)
	}
	return docs, nil
}

// SearchComplex 复杂搜索接口 (支持逻辑运算和位图过滤)
func (idx *Indexer) SearchComplex(query *types.TermQuery) ([]*types.Document, error) {
	resultSL := idx.reverseIndex.SearchQuery(query)
	if resultSL == nil || resultSL.Len() == 0 {
		return nil, nil
	}

	docs := make([]*types.Document, 0, resultSL.Len())
	for e := resultSL.Front(); e != nil; e = e.Next() {
		docId := e.Key().(uint64)

		var score float64 = 0
		if slv, ok := e.Value.(*types.SkipListValue); ok {
			score = float64(slv.Score)
		}
		doc, err := idx.GetDocument(docId)
		if err != nil {
			util.LogDebug("SearchComplex encountered error reading doc %d: %v", docId, err)
			continue
		}
		doc.Score = score
		docs = append(docs, doc)
	}
	return docs, nil
}

// GetSuggestions 获取搜索建议
func (idx *Indexer) GetSuggestions(prefix string) []string {
	return idx.reverseIndex.GetSuggestions(prefix)
}

// Close 关闭
func (idx *Indexer) Close() error {
	if closer, ok := idx.reverseIndex.(interface{ CloseWAL() error }); ok {
		_ = closer.CloseWAL()
	}
	return idx.forwardIndex.Close()
}

// BatchAddDocument 批量添加文档
func (idx *Indexer) BatchAddDocument(docs []*types.Document) ([]uint64, error) {
	if len(docs) == 0 {
		return nil, nil
	}

	var allKeys, allValues [][]byte
	var docIds []uint64

	for _, doc := range docs {
		if doc == nil || doc.IntId == 0 {
			continue
		}

		// 必须在序列化之前进行分词
		tokenizer := util.GetTokenizer()
		if doc.Title != "" {
			words := tokenizer.Cut(doc.Title)
			for _, w := range words {
				doc.Keyword = append(doc.Keyword, &types.Keyword{Field: "title", Word: w})
			}
		}
		if doc.Content != "" {
			words := tokenizer.Cut(doc.Content)
			for _, w := range words {
				doc.Keyword = append(doc.Keyword, &types.Keyword{Field: "content", Word: w})
			}
		}

		docBytes, err := proto.Marshal(doc)
		if err != nil {
			return nil, err
		}

		keyBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(keyBytes, doc.IntId)

		allKeys = append(allKeys, keyBytes)
		allValues = append(allValues, docBytes)
		docIds = append(docIds, doc.IntId)
	}

	if err := idx.forwardIndex.BatchSet(allKeys, allValues); err != nil {
		return nil, err
	}

	for _, doc := range docs {
		if err := idx.addReverseIndex(doc); err != nil {
			return nil, err
		}
	}
	return docIds, nil
}

// BatchDeleteDocument 批量删除文档
func (idx *Indexer) BatchDeleteDocument(docIds []uint64) error {
	if len(docIds) == 0 {
		return nil
	}

	keys := make([][]byte, 0, len(docIds))
	for _, id := range docIds {
		keyBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(keyBytes, id)
		keys = append(keys, keyBytes)
	}

	docBytesList, err := idx.forwardIndex.BatchGet(keys)
	if err != nil {
		return err
	}

	for _, docBytes := range docBytesList {
		if docBytes != nil {
			doc := &types.Document{}
			if err := proto.Unmarshal(docBytes, doc); err != nil {
				continue
			}
			for _, kw := range doc.Keyword {
				if kw != nil {
					if err := idx.reverseIndex.Delete(kw, doc.IntId); err != nil {
						return err
					}
				}
			}
		}
	}

	return idx.forwardIndex.BatchDelete(keys)
}

func (idx *Indexer) LoadFromForwardIndex() (int, error) {
	util.LogInfo("Loading from forward index...")

	count := 0
	if err := idx.forwardIndex.IterDB(func(key, value []byte) error {
		doc := &types.Document{}
		if err := proto.Unmarshal(value, doc); err != nil {
			util.LogError("Failed to unmarshal document: %v", err)
			return nil
		}
		if err := idx.addReverseIndex(doc); err != nil {
			return err
		}
		count++
		if count%1000 == 0 {
			util.LogInfo("Loaded %d documents", count)
		}
		return nil
	}); err != nil {
		return count, err
	}
	return count, nil
}

func splitKeyword(s string) (string, string, bool) {
	parts := strings.SplitN(s, "\x01", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", false
	}
	return parts[0], parts[1], true
}

func buildSkipListValue(doc *types.Document, field, word string) *types.SkipListValue {
	var count float32
	for _, kw := range doc.Keyword {
		if kw != nil && kw.Field == field && kw.Word == word {
			count++
		}
	}
	if count == 0 {
		count = 1
	}
	return &types.SkipListValue{
		Id:          doc.Id,
		BitsFeature: doc.BitsFeature,
		Score:       count,
	}
}

func (idx *Indexer) ReplayWALFrom(wal *kvdb.MmapWAL, offset uint64) (uint64, error) {
	if wal == nil {
		return offset, nil
	}
	lastSeq := offset
	applyEntry := func(entry *kvdb.WALEntry) error {
		if entry.SeqNum <= offset {
			return nil
		}
		field, word, ok := splitKeyword(entry.Keyword)
		if !ok {
			return nil
		}
		switch entry.ActionType {
		case 0:
			doc, err := idx.GetDocument(entry.DocID)
			if err != nil {
				return err
			}
			slv := buildSkipListValue(doc, field, word)
			if err := idx.reverseIndex.Add(&types.Keyword{Field: field, Word: word}, entry.DocID, slv); err != nil {
				return err
			}
		case 1:
			if err := idx.reverseIndex.Delete(&types.Keyword{Field: field, Word: word}, entry.DocID); err != nil {
				return err
			}
		}
		lastSeq = entry.SeqNum
		return nil
	}
	paths, err := kvdb.ListWALSegmentPaths(wal.Path())
	if err != nil {
		return lastSeq, err
	}
	for _, path := range paths {
		if path == wal.Path() {
			if err := wal.ReadAll(applyEntry); err != nil {
				return lastSeq, err
			}
			continue
		}
		seg, err := kvdb.OpenMmapWALForReplay(path)
		if err != nil {
			return lastSeq, err
		}
		err = seg.ReadAll(applyEntry)
		_ = seg.Close()
		if err != nil {
			return lastSeq, err
		}
	}
	return lastSeq, err
}
