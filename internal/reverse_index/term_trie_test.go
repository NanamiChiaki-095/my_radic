package reverseindex

import (
	"reflect"
	"testing"
)

func TestTermTrie_Basic(t *testing.T) {
	trie := NewTermTrie()

	// 1. 添加 "apple" 到 title
	trie.Add("apple", "title")

	// 2. 测试查找
	if !trie.Find("apple", "title") {
		t.Errorf("Should find 'apple' in title")
	}
	if trie.Find("apple", "content") {
		t.Errorf("Should NOT find 'apple' in content (field mismatch)")
	}
	if trie.Find("banana", "title") {
		t.Errorf("Should NOT find 'banana' (not exist)")
	}

	// 3. 测试掩码合并：再添加 "apple" 到 content
	trie.Add("apple", "content")
	if !trie.Find("apple", "title") {
		t.Errorf("Should find 'apple' in title after merge")
	}
	if !trie.Find("apple", "content") {
		t.Errorf("Should find 'apple' in content after merge")
	}
}

func TestTermTrie_Remove(t *testing.T) {
	trie := NewTermTrie()
	trie.Add("test", "title")
	trie.Add("test", "content")

	// 初始状态：Title 和 Content 都有
	if !trie.Find("test", "title") || !trie.Find("test", "content") {
		t.Fatal("Setup failed")
	}

	// 1. 移除 Title 标记
	trie.Remove("test", "title")

	if trie.Find("test", "title") {
		t.Errorf("Should NOT find 'test' in title after remove")
	}
	if !trie.Find("test", "content") {
		t.Errorf("Should STILL find 'test' in content")
	}

	// 2. 移除 Content 标记 (此时词应该彻底失效)
	trie.Remove("test", "content")
	if trie.Find("test", "content") {
		t.Errorf("Should NOT find 'test' in content after full remove")
	}
	// 验证 IsEnd 是否被取消
	trie.lock.RLock()
	node := trie.root
	for _, r := range "test" {
		node = node.Children[r]
	}
	if node.IsEnd {
		t.Errorf("Node IsEnd should be false after all fields removed")
	}
	trie.lock.RUnlock()
}

func TestTermTrie_Chinese(t *testing.T) {
	trie := NewTermTrie()
	trie.Add("搜索引擎", "title")
	trie.Add("搜索", "content")

	if !trie.Find("搜索引擎", "title") {
		t.Errorf("Should find Chinese term")
	}
	
	// 测试 Suggestion
	res := trie.GlobalSuggestion("搜")
	expected := []string{"搜索", "搜索引擎"}
	
	if len(res) != len(expected) {
		t.Errorf("Expected %d suggestions, got %d", len(expected), len(res))
	}
	
	// 验证内容 (因为 map 遍历顺序问题，这里只验证存在性，或者用 DeepEqual 如果 Trie 实现了排序)
	// 我们已经实现了排序，所以可以用 DeepEqual
	if !reflect.DeepEqual(res, expected) {
		t.Errorf("Chinese suggestion mismatch.\nExpected: %v\nGot:      %v", expected, res)
	}
}

func TestTermTrie_Suggestion_Order(t *testing.T) {
	trie := NewTermTrie()
	
	// 添加一组词
	words := []string{"golang", "google", "good", "go", "gopher"}
	for _, w := range words {
		trie.Add(w, "title")
	}

	// 测试 "go" 的提示
	res := trie.GlobalSuggestion("go")
	
	// 预期结果应该是按字典序排列的
	expected := []string{"go", "golang", "good", "google", "gopher"}
	
	if !reflect.DeepEqual(res, expected) {
		t.Errorf("Suggestion order mismatch.\nExpected: %v\nGot:      %v", expected, res)
	}
}

func TestTermTrie_FastReject(t *testing.T) {
	trie := NewTermTrie()
	trie.Add("exists", "title")

	// 场景 1: 词不存在
	if trie.Find("nonexist", "title") {
		t.Error("Should reject non-existent word")
	}

	// 场景 2: 词存在但字段不对
	if trie.Find("exists", "content") {
		t.Error("Should reject wrong field")
	}

	// 场景 3: 正常放行
	if !trie.Find("exists", "title") {
		t.Error("Should accept correct word and field")
	}
}
