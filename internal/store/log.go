// Package store 提供仅追加（append-only）事件日志。
//
// 日志以 JSONL 文件落盘，每条记录占一行并参与链式摘要：
// record_n 的摘要 = sha256(record_{n-1} 摘要 + 规范化后的事件 JSON)。
// 重放时逐行校验链，任何篡改、截断都能被发现。中心重启后从日志完整重建状态。
package store

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"example.com/batch-092001-q010/internal/domain"
)

// 初始链摘要，放在第一条记录之前，使链的起点固定且可核对。
const genesisDigest = "sha256:genesis-q010"

// Record 是日志中的一条持久化记录。
type Record struct {
	Offset      int64           `json:"offset"`       // 从 0 开始的行序号
	ChainDigest string          `json:"chain_digest"` // 截至本条的链式摘要
	Envelope    domain.Envelope `json:"envelope"`
}

// Log 是事件日志的抽象，文件实现与内存实现共用同一接口。
type Log interface {
	io.Closer
	// Append 把事件原子地追加到日志末尾，返回记录行序号。
	Append(env domain.Envelope) (int64, error)
	// Replay 按序号顺序把全部记录送给 fn；fn 返回错误会中断重放。
	Replay(fn func(Record) error) error
	// Len 返回已持久化的记录条数。
	Len() int64
}

// chainRecord 是磁盘上每行的实际 JSON 形状。
type chainRecord struct {
	Offset      int64           `json:"offset"`
	ChainDigest string          `json:"chain_digest"`
	Envelope    domain.Envelope `json:"envelope"`
}

// FileLog 把事件以 JSONL 形式追加到单个文件，每次写入后 fsync。
type FileLog struct {
	mu     sync.Mutex
	file   *os.File
	writer *bufio.Writer
	count  int64
	head   string // 当前链头摘要
}

// OpenFileLog 打开（或创建）日志文件并完整重放以校验链完整性。
func OpenFileLog(path string) (*FileLog, error) {
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("创建日志目录失败: %w", err)
		}
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("打开日志文件失败: %w", err)
	}
	log := &FileLog{file: file, head: genesisDigest}
	if err := log.verifyAndLoad(); err != nil {
		_ = file.Close()
		return nil, err
	}
	log.writer = bufio.NewWriter(file)
	return log, nil
}

// verifyAndLoad 逐行读取已有记录，校验链式摘要并恢复链头与计数。
func (l *FileLog) verifyAndLoad() error {
	if _, err := l.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	scanner := bufio.NewScanner(l.file)
	scanner.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	var index int64
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var rec chainRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			return fmt.Errorf("第 %d 行事件无法解析，日志可能损坏: %w", index, err)
		}
		if rec.Offset != index {
			return fmt.Errorf("第 %d 行序号异常: 记录为 %d", index, rec.Offset)
		}
		expected := linkDigest(l.head, rec.Envelope)
		if rec.ChainDigest != expected {
			return fmt.Errorf("第 %d 行链式摘要不匹配：期望 %s，实际 %s（日志被篡改或截断）",
				index, expected, rec.ChainDigest)
		}
		l.head = rec.ChainDigest
		index++
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("读取事件日志失败: %w", err)
	}
	if _, err := l.file.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	l.count = index
	return nil
}

// linkDigest 计算下一条记录的链式摘要。
func linkDigest(prev string, env domain.Envelope) string {
	ordered, err := json.Marshal(env)
	if err != nil {
		// domain.Envelope 只含可 JSON 序列化的字段，marshal 不应失败。
		panic(err)
	}
	sum := sha256.Sum256(append([]byte(prev), ordered...))
	return "sha256:" + hex.EncodeToString(sum[:])
}

// Append 实现 Log。
func (l *FileLog) Append(env domain.Envelope) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	next := linkDigest(l.head, env)
	rec := chainRecord{Offset: l.count, ChainDigest: next, Envelope: env}
	line, err := json.Marshal(rec)
	if err != nil {
		return 0, fmt.Errorf("序列化事件失败: %w", err)
	}
	if _, err := l.writer.Write(line); err != nil {
		return 0, fmt.Errorf("写入事件日志失败: %w", err)
	}
	if err := l.writer.WriteByte('\n'); err != nil {
		return 0, err
	}
	if err := l.writer.Flush(); err != nil {
		return 0, err
	}
	// fsync 保证断电/崩溃后已确认的事件仍在磁盘上，避免"虚假确认"。
	if err := l.file.Sync(); err != nil {
		return 0, fmt.Errorf("同步事件日志失败: %w", err)
	}
	l.head = next
	l.count++
	return rec.Offset, nil
}

// Replay 实现 Log。
func (l *FileLog) Replay(fn func(Record) error) error {
	l.mu.Lock()
	path := l.file.Name()
	l.mu.Unlock()
	// 重新以只读方式顺序读取，避免与追加指针互相干扰。
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var rec chainRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			return err
		}
		if err := fn(Record{Offset: rec.Offset, ChainDigest: rec.ChainDigest, Envelope: rec.Envelope}); err != nil {
			return err
		}
	}
	return scanner.Err()
}

// Len 实现 Log。
func (l *FileLog) Len() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.count
}

// Close 刷新并关闭底层文件。
func (l *FileLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.writer != nil {
		if err := l.writer.Flush(); err != nil {
			return err
		}
	}
	return l.file.Close()
}

// MemoryLog 是纯内存日志，用于测试与未配置持久化路径时的运行。
type MemoryLog struct {
	mu      sync.Mutex
	records []Record
	head    string
}

// NewMemoryLog 创建空的内存日志。
func NewMemoryLog() *MemoryLog {
	return &MemoryLog{head: genesisDigest}
}

// Append 实现 Log。
func (m *MemoryLog) Append(env domain.Envelope) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	next := linkDigest(m.head, env)
	offset := int64(len(m.records))
	m.records = append(m.records, Record{Offset: offset, ChainDigest: next, Envelope: env})
	m.head = next
	return offset, nil
}

// Replay 实现 Log。
func (m *MemoryLog) Replay(fn func(Record) error) error {
	m.mu.Lock()
	snapshot := make([]Record, len(m.records))
	copy(snapshot, m.records)
	m.mu.Unlock()
	for _, rec := range snapshot {
		if err := fn(rec); err != nil {
			return err
		}
	}
	return nil
}

// Len 实现 Log。
func (m *MemoryLog) Len() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return int64(len(m.records))
}

// Close 实现 Log，内存模式为空操作。
func (m *MemoryLog) Close() error { return nil }

// ErrNotExist 等通用错误占位，便于上层区分。
var ErrNotExist = errors.New("记录不存在")
