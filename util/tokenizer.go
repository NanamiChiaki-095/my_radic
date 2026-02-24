package util

import (
	"os"
	"strings"
	"sync"

	"github.com/wangbin/jiebago"
)

type Tokenizer interface {
	Cut(text string) []string
}

type JiebaTokenizer struct {
	seg jiebago.Segmenter
}

var (
	globalTokenizer Tokenizer
	tokenizerOnce   sync.Once
)

func GetTokenizer() Tokenizer {
	tokenizerOnce.Do(func() {
		jt := &JiebaTokenizer{}
		dictPath := "dict.txt"
		// 简单的路径探测：如果当前目录没有，尝试去上一级找（适合在 internal/xxx 下跑测试的情况）
		if _, err := os.Stat(dictPath); os.IsNotExist(err) {
			dictPath = "../dict.txt"
			if _, err := os.Stat(dictPath); os.IsNotExist(err) {
				dictPath = "../../dict.txt"
			}
		}

		if err := jt.seg.LoadDictionary(dictPath); err != nil {
			LogWarn("LoadDictionary %s failed: %v, using default", dictPath, err)
		}
		globalTokenizer = jt
	})
	return globalTokenizer
}

func (t *JiebaTokenizer) Cut(text string) []string {
	text = strings.ToLower(text)

	ch := t.seg.Cut(text, true)

	results := make([]string, 0, len(ch))
	for word := range ch {
		word = strings.TrimSpace(word)
		if len(word) > 1 {
			results = append(results, word)
		}
	}
	return results
}
