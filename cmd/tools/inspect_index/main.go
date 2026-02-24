package main

import (
	"flag"
	"fmt"
	"my_radic/internal/indexer"
	"my_radic/internal/kvdb"
	reverseindex "my_radic/internal/reverse_index"
	"my_radic/types"
	"os"
	"path/filepath"
)

func main() {
	dbPathPtr := flag.String("db", "./data/radic.db", "Path to BoltDB file")
	queryPtr := flag.String("query", "", "Term to search (optional)")
	docIDPtr := flag.Uint64("doc", 0, "DocID to lookup content (optional)")
	flag.Parse()

	// 1. 初始化数据库
	// 注意处理 Windows 路径分隔符
	realPath := filepath.Clean(*dbPathPtr)
	if _, err := os.Stat(realPath); os.IsNotExist(err) {
		fmt.Printf("Error: Database file not found at %s\n", realPath)
		return
	}

	boltDB := kvdb.NewBolt().WithDataPath(realPath)
	if err := boltDB.WithBucket("doc_store").Open(); err != nil {
		fmt.Printf("Error opening BoltDB: %v\n", err)
		return
	}
	defer boltDB.Close()

	// 2. 初始化索引器 (只读模式，不需要 Kafka)
	revIdx := reverseindex.NewSkipListReverseIndex(10000)
	idx := indexer.NewIndexer(boltDB, revIdx)

	fmt.Println("Loading index from BoltDB...")
	count, err := idx.LoadFromForwardIndex()
	if err != nil {
		fmt.Printf("Error loading index: %v\n", err)
		return
	}
	fmt.Printf("Index loaded. Total documents: %d\n", count)

	// 3. 执行操作
	// A. 查文档内容
	if *docIDPtr > 0 {
		fmt.Printf("\n--- Inspecting DocID: %d ---\n", *docIDPtr)
		docBytes, err := boltDB.Get([]byte(fmt.Sprintf("%d", *docIDPtr)))
		if err != nil {
			fmt.Printf("Error reading doc: %v\n", err)
		} else if len(docBytes) == 0 {
			fmt.Println("Document not found.")
		} else {
			// 尝试反序列化为 Document Proto
			// 注意：这里假设 BoltDB 存的是 protobuf 序列化后的数据
			// 如果存的是 JSON 或其他格式，需要相应调整。
			// 假设 KVDB 存的是 types.Document 的 protobuf 字节流
			// 但实际上 KVDB 存的可能是业务层的 Article JSON？
			// 回看 bolt_db.go 和 indexer.go，Indexer 是把 proto 序列化存进去的吗？
			// 检查 indexer.go 的 AddDocument 实现：
			// 它是把 doc 存入 ForwardIndex。
			fmt.Printf("Raw bytes length: %d\n", len(docBytes))
		}
	}

	// B. 搜索关键词
	if *queryPtr != "" {
		fmt.Printf("\n--- Searching for term: '%s' ---\n", *queryPtr)
		// 构造一个简单的 TermQuery
		q := &types.TermQuery{
			Keyword: &types.Keyword{
				Field: "content", // 默认搜 content 字段
				Word:  *queryPtr,
			},
		}

		results, err := idx.SearchComplex(q)
		if err != nil {
			fmt.Printf("Search failed: %v\n", err)
		} else {
			fmt.Printf("Found %d matches:\n", len(results))
			for i, doc := range results {
				if i >= 5 {
					fmt.Printf("... and %d more\n", len(results)-5)
					break
				}
				fmt.Printf("[%d] ID=%s Score=%.4f Title=%s\n", i+1, doc.Id, doc.Score, doc.Title)
			}
		}
	}
}
