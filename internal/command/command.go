// Package command 帮助现场终端构造符合契约的事件信封：
// 自动填充 schema_version、对载荷做规范化并计算 sha256 摘要。
package command

import (
	"fmt"
	"time"

	"example.com/batch-092001-q010/internal/domain"
)

// Options 是构造事件时的可选项。
type Options struct {
	DeviceSerial string // 相关设备序号（阶段变化、遥测等必填）
	Operator     string // 触发/签署该事件的岗位引用
}

// Build 构造一个可直接提交给 /v1/events/ingest 的事件信封。
// sourceSequence 是该来源内的连续序号，occurredAt 为事件在现场发生的时刻。
func Build(eventID, subjectRef, eventType, source string, sourceSequence int64, occurredAt time.Time, payload any, opts Options) (domain.Envelope, error) {
	raw, digest, err := domain.CanonicalDigest(payload)
	if err != nil {
		return domain.Envelope{}, fmt.Errorf("规范化载荷失败: %w", err)
	}
	return domain.Envelope{
		SchemaVersion:  domain.SchemaVersion,
		EventID:        eventID,
		SubjectRef:     subjectRef,
		Type:           eventType,
		OccurredAt:     occurredAt.Format(time.RFC3339),
		Source:         source,
		SourceSequence: sourceSequence,
		DeviceSerial:   opts.DeviceSerial,
		Operator:       opts.Operator,
		Payload:        raw,
		PayloadDigest:  digest,
	}, nil
}
