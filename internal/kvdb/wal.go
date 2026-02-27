package kvdb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"my_radic/util"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// WALMagic 用于识别文件类型
	WALMagic = "WAL1"
	// WALVersion 记录格式版本
	WALVersion = 1
	// WALHeaderSize 固定头部长度
	WALHeaderSize = 32
	// WALDefaultSize 初始文件大小
	WALDefaultSize = 64 * 1024 * 1024
	// WALExpandSize 扩容步长
	WALExpandSize = 32 * 1024 * 1024
)

type WALEntry struct {
	// SeqNum 单调递增序列号
	SeqNum uint64
	// ActionType 0=ADD, 1=DEL
	ActionType uint8
	// Keyword 关键词
	Keyword string
	// DocID 文档内部 ID
	DocID uint64
	// Value 记录载荷
	Value []byte
}

func (w *MmapWAL) replicateWAL(seq uint64) error {
	if seq <= w.seqNum {
		return nil
	}
	return nil
}

type MmapWAL struct {
	// data mmap 映射后的内存区域
	data []byte
	// file 对应的文件句柄
	file *os.File
	// fileSize 当前文件大小
	fileSize int64
	// writeOffset 当前写入偏移
	writeOffset int64
	// seqNum 最后一次写入的序列号
	seqNum   uint64
	initSize int64

	// flushInterval 定时刷盘周期
	flushInterval time.Duration
	// lastFlush 上一次刷盘时间
	lastFlush time.Time
	// mu 保护写入与刷盘
	mu sync.Mutex
	// closed 关闭标志
	closed int32
	//记录WAL文件路径
	path string
	//单个WAL的最大允许大小
	maxSize int64
	//保留的历史WAL文件数量
	maxFiles int
	// closeChan 用于退出刷盘协程
	closeChan chan struct{}
	// syncFunc 刷盘函数
	syncFunc func() error
	// unmapFunc 解除映射
	unmapFunc func() error
}

const (
	// 文件头布局
	walHeaderMagicOffset   = 0
	walHeaderVersionOffset = 4
	walHeaderFileSize      = 8
	walHeaderWriteOffset   = 16
	walHeaderSeqNum        = 24

	// 记录布局
	walRecordLenSize     = 4
	walRecordCrcSize     = 4
	walRecordSeqSize     = 8
	walRecordActionSize  = 1
	walRecordKeywordSize = 2
	walRecordDocIDSize   = 8
	walRecordValueSize   = 4
	walRecordHeaderSize  = walRecordCrcSize + walRecordSeqSize + walRecordActionSize + walRecordKeywordSize + walRecordDocIDSize + walRecordValueSize
)

var (
	// errWALClosed 已关闭
	errWALClosed = errors.New("wal is closed")
	// errWALCorrupted 记录损坏
	errWALCorrupted = errors.New("wal record corrupted")
	// errWALInvalidEntry 无效写入
	errWALInvalidEntry = errors.New("wal entry is invalid")
)

// OpenMmapWAL 打开或创建 WAL 文件，并完成 mmap 映射与恢复
func OpenMmapWAL(path string, size int64, flushInterval time.Duration) (*MmapWAL, error) {
	if size <= 0 {
		size = WALDefaultSize
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	fileSize := stat.Size()
	// 文件过小时直接扩到预期大小
	if fileSize < WALHeaderSize {
		fileSize = size
		if err := f.Truncate(fileSize); err != nil {
			f.Close()
			return nil, err
		}
	}

	data, syncFunc, unmapFunc, err := mapFile(f, fileSize)
	if err != nil {
		f.Close()
		return nil, err
	}

	//初始化maxSize和maxFiles字段
	w := &MmapWAL{
		data:          data,
		file:          f,
		fileSize:      fileSize,
		flushInterval: flushInterval,
		lastFlush:     time.Now(),
		closeChan:     make(chan struct{}),
		syncFunc:      syncFunc,
		unmapFunc:     unmapFunc,
		path:          path,
		initSize:      size,
		maxSize:       0,
		maxFiles:      0,
	}

	// 首次初始化或非法头部时重置头
	if !w.hasValidHeader() {
		w.writeOffset = WALHeaderSize
		w.seqNum = 0
		w.writeHeader()
	} else {
		w.readHeader()
	}

	// 恢复写指针与序列号
	if err := w.recoverOffsets(); err != nil {
		w.Close()
		return nil, err
	}

	// 定时刷盘
	if flushInterval > 0 {
		go w.flushLoop()
	}

	return w, nil
}

func OpenMmapWALWithRotate(path string, size int64, flushInterval time.Duration, maxSize int64, maxFiles int) (*MmapWAL, error) {
	w, err := OpenMmapWAL(path, size, flushInterval)
	if err != nil {
		return nil, err
	}
	if maxSize > 0 {
		w.maxSize = maxSize
	} else {
		w.maxSize = w.fileSize
	}
	if maxFiles > 0 {
		w.maxFiles = maxFiles
	} else {
		w.maxFiles = 3
	}
	return w, nil
}

func OpenMmapWALForReplay(path string) (*MmapWAL, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	fileSize := stat.Size()
	if fileSize < WALHeaderSize {
		f.Close()
		return nil, errors.New("wal file too small")
	}
	data, syncFunc, unmapFunc, err := mapFile(f, fileSize)
	if err != nil {
		f.Close()
		return nil, err
	}
	w := &MmapWAL{
		data:          data,
		file:          f,
		fileSize:      fileSize,
		flushInterval: 0,
		lastFlush:     time.Now(),
		closeChan:     make(chan struct{}),
		syncFunc:      syncFunc,
		unmapFunc:     unmapFunc,
		path:          path,
		initSize:      fileSize,
		maxSize:       0,
		maxFiles:      0,
	}
	if !w.hasValidHeader() {
		w.Close()
		return nil, errors.New("wal header invalid")
	}
	w.readHeader()
	if err := w.recoverOffsets(); err != nil {
		w.Close()
		return nil, err
	}
	return w, nil
}

func ListWALSegmentPaths(basePath string) ([]string, error) {
	dir := filepath.Dir(basePath)
	base := filepath.Base(basePath)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	type seg struct {
		path string
		ts   int64
		name string
	}
	prefix := base + "."
	var segments []seg
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		tsStr := strings.TrimPrefix(name, prefix)
		ts, err := strconv.ParseInt(tsStr, 10, 64)
		if err != nil {
			ts = 0
		}
		segments = append(segments, seg{
			path: filepath.Join(dir, name),
			ts:   ts,
			name: name,
		})
	}
	sort.Slice(segments, func(i, j int) bool {
		if segments[i].ts == segments[j].ts {
			return segments[i].name < segments[j].name
		}
		return segments[i].ts < segments[j].ts
	})
	paths := make([]string, 0, len(segments)+1)
	for _, s := range segments {
		paths = append(paths, s.path)
	}
	paths = append(paths, basePath)
	return paths, nil
}

func (w *MmapWAL) Path() string {
	return w.path
}

func (w *MmapWAL) CurrentSeq() uint64 {
	if w == nil {
		return 0
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.seqNum
}

// Close 关闭 WAL，刷盘并解除映射
func (w *MmapWAL) Close() error {
	if !atomic.CompareAndSwapInt32(&w.closed, 0, 1) {
		return nil
	}
	close(w.closeChan)
	w.mu.Lock()
	defer w.mu.Unlock()
	// 关闭前先更新头部，再刷盘
	w.writeHeader()
	if err := w.syncFunc(); err != nil {
		return err
	}
	if err := w.unmapFunc(); err != nil {
		return err
	}
	if w.file != nil {
		return w.file.Close()
	}
	return nil
}

// Append 追加一条记录
func (w *MmapWAL) Append(entry *WALEntry) (uint64, error) {
	if entry == nil {
		return 0, errWALInvalidEntry
	}
	if atomic.LoadInt32(&w.closed) == 1 {
		return 0, errWALClosed
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	// 分配序列号
	seq := w.seqNum + 1
	entry.SeqNum = seq

	// 计算记录大小
	keywordBytes := []byte(entry.Keyword)
	valueBytes := entry.Value
	recordLen := walRecordHeaderSize + len(keywordBytes) + len(valueBytes)
	totalLen := walRecordLenSize + recordLen

	if w.maxSize > 0 && w.writeOffset+int64(totalLen) > w.maxSize {
		if err := w.rotateLocked(); err != nil {
			return 0, err
		}
	}

	// 扩容检查
	if err := w.ensureCapacity(int64(totalLen)); err != nil {
		return 0, err
	}

	// 写入 record header
	offset := w.writeOffset
	buf := w.data[offset : offset+int64(totalLen)]
	binary.BigEndian.PutUint32(buf[0:4], uint32(recordLen))

	pos := walRecordLenSize
	binary.BigEndian.PutUint32(buf[pos:pos+walRecordCrcSize], 0)
	pos += walRecordCrcSize
	binary.BigEndian.PutUint64(buf[pos:pos+walRecordSeqSize], seq)
	pos += walRecordSeqSize
	buf[pos] = entry.ActionType
	pos += walRecordActionSize
	binary.BigEndian.PutUint16(buf[pos:pos+walRecordKeywordSize], uint16(len(keywordBytes)))
	pos += walRecordKeywordSize
	binary.BigEndian.PutUint64(buf[pos:pos+walRecordDocIDSize], entry.DocID)
	pos += walRecordDocIDSize
	binary.BigEndian.PutUint32(buf[pos:pos+walRecordValueSize], uint32(len(valueBytes)))
	pos += walRecordValueSize

	// 写入 payload
	copy(buf[pos:pos+len(keywordBytes)], keywordBytes)
	pos += len(keywordBytes)
	copy(buf[pos:pos+len(valueBytes)], valueBytes)

	// 计算并写入 CRC
	crc := crc32.ChecksumIEEE(buf[walRecordLenSize+walRecordCrcSize : totalLen])
	binary.BigEndian.PutUint32(buf[walRecordLenSize:walRecordLenSize+walRecordCrcSize], crc)

	// 更新元数据并刷头
	w.writeOffset += int64(totalLen)
	w.seqNum = seq
	w.writeHeader()

	// 按时间触发刷盘
	if w.flushInterval > 0 && time.Since(w.lastFlush) >= w.flushInterval {
		if err := w.syncFunc(); err != nil {
			return seq, err
		}
		w.lastFlush = time.Now()
	}

	return seq, nil
}

func (w *MmapWAL) Rotate() error {
	if atomic.LoadInt32(&w.closed) == 1 {
		return errWALClosed
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.rotateLocked()
}

func (w *MmapWAL) rotateLocked() error {
	if w.file == nil {
		return errWALClosed
	}
	w.writeHeader()
	if err := w.syncFunc(); err != nil {
		return err
	}
	if err := w.unmapFunc(); err != nil {
		return err
	}
	if err := w.file.Close(); err != nil {
		return err
	}

	dir := filepath.Dir(w.path)
	base := filepath.Base(w.path)
	rotatedPath := filepath.Join(dir, fmt.Sprintf("%s.%d", base, time.Now().UnixNano()))
	if err := os.Rename(w.path, rotatedPath); err != nil {
		if reopenErr := w.reopenOriginal(); reopenErr != nil {
			return err
		}
		return err
	}

	fileSize := w.initSize
	if fileSize <= 0 {
		fileSize = WALDefaultSize
	}
	f, err := os.OpenFile(w.path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	if err := f.Truncate(fileSize); err != nil {
		f.Close()
		return err
	}
	data, syncFunc, unmapFunc, err := mapFile(f, fileSize)
	if err != nil {
		f.Close()
		return err
	}
	w.data = data
	w.file = f
	w.fileSize = fileSize
	w.syncFunc = syncFunc
	w.unmapFunc = unmapFunc
	w.writeOffset = WALHeaderSize
	w.writeHeader()

	if w.maxFiles > 0 {
		w.cleanupOldWALLocked(dir, base)
	}
	return nil
}

func (w *MmapWAL) reopenOriginal() error {
	f, err := os.OpenFile(w.path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	data, syncFunc, unmapFunc, err := mapFile(f, stat.Size())
	if err != nil {
		f.Close()
		return err
	}
	w.data = data
	w.file = f
	w.fileSize = stat.Size()
	w.syncFunc = syncFunc
	w.unmapFunc = unmapFunc
	w.readHeader()
	return w.recoverOffsets()
}

// ReadAll 顺序读取所有合法记录
func (w *MmapWAL) ReadAll(fn func(*WALEntry) error) error {
	if fn == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	offset := int64(WALHeaderSize)
	for offset+walRecordLenSize <= w.writeOffset {
		// 读取长度并做基本校验
		recordLen := int64(binary.BigEndian.Uint32(w.data[offset : offset+walRecordLenSize]))
		if recordLen <= 0 {
			return nil
		}
		totalLen := walRecordLenSize + recordLen
		if offset+totalLen > w.writeOffset {
			return errWALCorrupted
		}

		buf := w.data[offset : offset+totalLen]
		// CRC 校验
		crc := binary.BigEndian.Uint32(buf[walRecordLenSize : walRecordLenSize+walRecordCrcSize])
		if crc32.ChecksumIEEE(buf[walRecordLenSize+walRecordCrcSize:]) != crc {
			return errWALCorrupted
		}

		pos := walRecordLenSize + walRecordCrcSize
		seq := binary.BigEndian.Uint64(buf[pos : pos+walRecordSeqSize])
		pos += walRecordSeqSize
		action := buf[pos]
		pos += walRecordActionSize
		keywordLen := int(binary.BigEndian.Uint16(buf[pos : pos+walRecordKeywordSize]))
		pos += walRecordKeywordSize
		docID := binary.BigEndian.Uint64(buf[pos : pos+walRecordDocIDSize])
		pos += walRecordDocIDSize
		valueLen := int(binary.BigEndian.Uint32(buf[pos : pos+walRecordValueSize]))
		pos += walRecordValueSize

		// 长度边界校验
		if pos+keywordLen+valueLen > len(buf) {
			return errWALCorrupted
		}

		// 反序列化记录
		entry := &WALEntry{
			SeqNum:     seq,
			ActionType: action,
			Keyword:    string(buf[pos : pos+keywordLen]),
			DocID:      docID,
			Value:      append([]byte(nil), buf[pos+keywordLen:pos+keywordLen+valueLen]...),
		}
		if err := fn(entry); err != nil {
			return err
		}

		offset += totalLen
	}
	return nil
}

// Flush 主动刷盘
func (w *MmapWAL) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.writeHeader()
	if err := w.syncFunc(); err != nil {
		return err
	}
	w.lastFlush = time.Now()
	return nil
}

// hasValidHeader 校验头部合法性
func (w *MmapWAL) hasValidHeader() bool {
	if len(w.data) < WALHeaderSize {
		return false
	}
	if string(w.data[walHeaderMagicOffset:walHeaderMagicOffset+4]) != WALMagic {
		return false
	}
	version := binary.BigEndian.Uint32(w.data[walHeaderVersionOffset : walHeaderVersionOffset+4])
	return version == WALVersion
}

// readHeader 读取头部信息到内存
func (w *MmapWAL) readHeader() {
	w.fileSize = int64(binary.BigEndian.Uint64(w.data[walHeaderFileSize : walHeaderFileSize+8]))
	w.writeOffset = int64(binary.BigEndian.Uint64(w.data[walHeaderWriteOffset : walHeaderWriteOffset+8]))
	w.seqNum = binary.BigEndian.Uint64(w.data[walHeaderSeqNum : walHeaderSeqNum+8])
	if w.writeOffset < WALHeaderSize {
		w.writeOffset = WALHeaderSize
	}
}

// writeHeader 将内存中的元数据写回头部
func (w *MmapWAL) writeHeader() {
	if len(w.data) < WALHeaderSize {
		return
	}
	copy(w.data[walHeaderMagicOffset:walHeaderMagicOffset+4], []byte(WALMagic))
	binary.BigEndian.PutUint32(w.data[walHeaderVersionOffset:walHeaderVersionOffset+4], WALVersion)
	binary.BigEndian.PutUint64(w.data[walHeaderFileSize:walHeaderFileSize+8], uint64(w.fileSize))
	binary.BigEndian.PutUint64(w.data[walHeaderWriteOffset:walHeaderWriteOffset+8], uint64(w.writeOffset))
	binary.BigEndian.PutUint64(w.data[walHeaderSeqNum:walHeaderSeqNum+8], w.seqNum)
}

// recoverOffsets 通过扫描记录恢复写指针与序列号
func (w *MmapWAL) recoverOffsets() error {
	offset := int64(WALHeaderSize)
	seq := w.seqNum

	for offset+walRecordLenSize <= int64(len(w.data)) {
		recordLen := int64(binary.BigEndian.Uint32(w.data[offset : offset+walRecordLenSize]))
		if recordLen == 0 {
			break
		}
		totalLen := walRecordLenSize + recordLen
		if offset+totalLen > int64(len(w.data)) {
			break
		}

		buf := w.data[offset : offset+totalLen]
		crc := binary.BigEndian.Uint32(buf[walRecordLenSize : walRecordLenSize+walRecordCrcSize])
		if crc32.ChecksumIEEE(buf[walRecordLenSize+walRecordCrcSize:]) != crc {
			break
		}

		pos := walRecordLenSize + walRecordCrcSize
		seq = binary.BigEndian.Uint64(buf[pos : pos+walRecordSeqSize])

		offset += totalLen
	}

	w.writeOffset = offset
	w.seqNum = seq
	w.writeHeader()
	return nil
}

// ensureCapacity 保证剩余空间足够写入
func (w *MmapWAL) ensureCapacity(need int64) error {
	if w.writeOffset+need <= w.fileSize {
		return nil
	}
	newSize := w.fileSize
	for w.writeOffset+need > newSize {
		newSize += WALExpandSize
	}
	if err := w.resize(newSize); err != nil {
		return err
	}
	return nil
}

// resize 扩容并重新映射
func (w *MmapWAL) resize(size int64) error {
	if err := w.unmapFunc(); err != nil {
		return err
	}
	if err := w.file.Truncate(size); err != nil {
		return err
	}
	data, syncFunc, unmapFunc, err := mapFile(w.file, size)
	if err != nil {
		return err
	}
	w.data = data
	w.syncFunc = syncFunc
	w.unmapFunc = unmapFunc
	w.fileSize = size
	w.writeHeader()
	return nil
}

// flushLoop 定时刷盘循环
func (w *MmapWAL) flushLoop() {
	ticker := time.NewTicker(w.flushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if atomic.LoadInt32(&w.closed) == 1 {
				return
			}
			w.Flush()
		case <-w.closeChan:
			return
		}
	}
}

func (w *MmapWAL) cleanupOldWALLocked(dir string, base string) {
	files, err := os.ReadDir(dir)
	if err != nil {
		util.LogError("MmapWAL cleanupOldWAL ReadDir err: %v", err)
		return
	}
	prefix := base + "."
	var candidates []string
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		name := file.Name()
		if strings.HasPrefix(name, prefix) {
			candidates = append(candidates, name)
		}
	}
	if len(candidates) <= w.maxFiles {
		return
	}
	sort.Strings(candidates)
	for i := 0; i < len(candidates)-w.maxFiles; i++ {
		if err := os.Remove(filepath.Join(dir, candidates[i])); err != nil {
			util.LogError("MmapWAL cleanupOldWAL Remove err: %v", err)
		}
	}
}
