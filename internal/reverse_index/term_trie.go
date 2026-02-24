package reverseindex

import (
	"sort"
	"sync"
)

// 字段位掩码常量定义 (BitMask)
// 使用位运算存储词项在不同字段中的出现情况，极致节省空间
const (
	FieldTitle   uint8 = 1 << 0 // 1: 标题字段
	FieldContent uint8 = 1 << 1 // 2: 内容字段
	FieldTag     uint8 = 1 << 2 // 4: 标签字段
	FieldAuthor  uint8 = 1 << 3 // 8: 作者字段
)

// GetFieldMask 将字符串字段名映射为位掩码
func GetFieldMask(field string) uint8 {
	switch field {
	case "title":
		return FieldTitle
	case "content":
		return FieldContent
	case "tag":
		return FieldTag
	case "author":
		return FieldAuthor
	default:
		return 0
	}
}

// TrieNode 字典树节点结构
type TrieNode struct {
	Children  map[rune]*TrieNode // 子节点映射，使用 rune 支持中文
	IsEnd     bool               // 标记是否为一个完整单词的结尾
	FieldMask uint8              // 位图：记录该单词在哪些字段中出现过
}

// NewTrieNode 创建一个新的字典树节点
func NewTrieNode() *TrieNode {
	return &TrieNode{
		Children:  make(map[rune]*TrieNode),
		IsEnd:     false,
		FieldMask: 0,
	}
}

// TermTrie 并发安全的字典树实现
// 用于：1. 全局搜索提示 (Suggestion) 2. 搜索请求的快速熔断/拦截 (Fast Reject)
type TermTrie struct {
	root *TrieNode
	lock sync.RWMutex
}

// NewTermTrie 初始化字典树
func NewTermTrie() *TermTrie {
	return &TermTrie{
		root: NewTrieNode(),
	}
}

// Add 向字典树添加词项，并标记其所属字段
func (t *TermTrie) Add(term string, field string) {
	if term == "" {
		return
	}
	mask := GetFieldMask(field)
	if mask == 0 {
		return
	}
	t.lock.Lock()
	defer t.lock.Unlock()

	node := t.root
	runes := []rune(term) // 将字符串转为 rune 数组，确保正确处理中文字符
	for _, r := range runes {
		if node.Children[r] == nil {
			node.Children[r] = NewTrieNode()
		}
		node = node.Children[r]
	}
	node.IsEnd = true
	node.FieldMask |= mask // 使用按位或(OR)合并字段信息
}

// Remove 从词项中移除特定字段的标记
func (t *TermTrie) Remove(term string, field string) {
	if term == "" {
		return
	}
	mask := GetFieldMask(field)

	t.lock.Lock()
	defer t.lock.Unlock()

	node := t.root
	runes := []rune(term)
	for _, r := range runes {
		if node.Children[r] == nil {
			return // 词项不存在，直接返回
		}
		node = node.Children[r]
	}
	// 使用位清零操作符 &^ 移除特定字段位
	node.FieldMask &^= mask
	// 如果所有字段标记都已清除，则该词项不再作为有效结尾
	if node.FieldMask == 0 {
		node.IsEnd = false
	}
}

// Find 精确查找词项，并验证字段匹配情况 (流量拦截的核心逻辑)
// 返回 true 表示该词在指定字段中确实存在
func (t *TermTrie) Find(term string, field string) bool {
	if term == "" {
		return false
	}
	mask := GetFieldMask(field)

	t.lock.RLock()
	defer t.lock.RUnlock()

	node := t.root
	runes := []rune(term)
	for _, r := range runes {
		if node.Children[r] == nil {
			return false // 路径中断，说明词项不存在
		}
		node = node.Children[r]
	}

	if !node.IsEnd {
		return false
	}
	// 位运算检查：目标字段位是否在 FieldMask 中
	return node.FieldMask&mask != 0
}

// GlobalSuggestion 全局搜索提示，忽略字段差异，返回所有匹配前缀的词
func (t *TermTrie) GlobalSuggestion(prefix string) []string {
	t.lock.RLock()
	defer t.lock.RUnlock()

	node := t.root
	runes := []rune(prefix)

	// 1. 先定位到前缀所在的节点
	for _, r := range runes {
		if node.Children[r] == nil {
			return nil
		}
		node = node.Children[r]
	}

	var result []string
	t.dfs(node, prefix, &result)
	return result
}

// dfs 深度优先遍历收集所有以 prefix 为前缀的单词
func (t *TermTrie) dfs(node *TrieNode, prefix string, result *[]string) {
	if node.IsEnd {
		*result = append(*result, prefix)
	}
	// 限制结果数量，防止搜索提示返回过多内容影响性能
	if len(*result) >= 10 {
		return
	}

	// 提取子节点并排序，确保 Suggestion 结果的顺序是稳定的 (Deterministic)
	keys := make([]rune, 0, len(node.Children))
	for r := range node.Children {
		keys = append(keys, r)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	for _, r := range keys {
		t.dfs(node.Children[r], prefix+string(r), result)
		if len(*result) >= 10 {
			return
		}
	}
}