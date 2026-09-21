package store

import (
	"path/filepath"
	"testing"
	"time"

	"example.com/batch-092001-q010/internal/domain"
)

func sampleEvent(id string, seq int64) domain.Envelope {
	return domain.Envelope{
		SchemaVersion: domain.SchemaVersion, EventID: id, SubjectRef: "SUBJ-" + id,
		Type: domain.TypeDroneRegistered, OccurredAt: time.Date(2026, 9, 21, 8, 0, 0, 0, time.UTC).Format(time.RFC3339),
		Source: "uav-1", SourceSequence: seq, DeviceSerial: "DRONE-1",
		Payload:       []byte(`{"drone_serial":"DRONE-1"}`),
		PayloadDigest: "sha256:ignored-on-write",
	}
}

func TestFileLogAppendReplayAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	log, err := OpenFileLog(path)
	if err != nil {
		t.Fatal(err)
	}
	for i := int64(1); i <= 3; i++ {
		if _, err := log.Append(sampleEvent("E"+string(rune('0'+i)), i)); err != nil {
			t.Fatal(err)
		}
	}
	if log.Len() != 3 {
		t.Fatalf("Len=%d", log.Len())
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}

	// 重新打开：链式摘要全部校验通过，水位恢复。
	reopened, err := OpenFileLog(path)
	if err != nil {
		t.Fatalf("正常日志重开必须通过: %v", err)
	}
	defer reopened.Close()
	if reopened.Len() != 3 {
		t.Fatalf("重开后 Len=%d", reopened.Len())
	}
	count := 0
	if err := reopened.Replay(func(rec Record) error {
		if rec.Envelope.SourceSequence != rec.Offset+1 {
			t.Fatal("记录序号与 offset 不一致")
		}
		count++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Fatalf("重放条数=%d", count)
	}
}

func TestMemoryLogOrdering(t *testing.T) {
	log := NewMemoryLog()
	for i := int64(1); i <= 2; i++ {
		if _, err := log.Append(sampleEvent("M", i)); err != nil {
			t.Fatal(err)
		}
	}
	seqs := []int64{}
	offsets := []int64{}
	_ = log.Replay(func(rec Record) error {
		seqs = append(seqs, rec.Envelope.SourceSequence)
		offsets = append(offsets, rec.Offset)
		return nil
	})
	if len(seqs) != 2 || seqs[0] != 1 || seqs[1] != 2 || offsets[0] != 0 || offsets[1] != 1 {
		t.Fatalf("追加顺序异常 seqs=%v offsets=%v", seqs, offsets)
	}
}
