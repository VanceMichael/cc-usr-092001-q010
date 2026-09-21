// Package domain 定义灾区空中通信任务控制器的事件契约、受控词汇与摘要规则。
//
// 系统中每一条业务事实都是一个仅追加的事件（Envelope）。事件信封携带稳定
// 标识、发生时间、来源序号与载荷摘要：来源序号只在同一来源内连续递增，
// 接收方必须保留原始 occurred_at，不得用到达时间覆盖。
package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"
)

// SchemaVersion 为当前事件契约版本，随载荷结构的不兼容变化而提升。
const SchemaVersion = "1"

// 岗位角色（role）。改派必须由事件载荷中声明的确认岗位确认，其他岗位无权确认。
const (
	RoleCommander  = "COMMANDER"   // 总指挥
	RoleAirspace   = "AIRSPACE"    // 空管/空域协调
	RoleFlightOps  = "FLIGHT_OPS"  // 飞行操控
	RoleNetworkOps = "NETWORK_OPS" // 空中基站/网络保障
	RoleGroundTeam = "GROUND_TEAM" // 地面队伍
	RoleSystem     = "SYSTEM"      // 系统自动策略（事件来源，不是人工岗位）
)

// 任务阶段（phase）。阶段推进只能由带设备序号与发生时间的显式事件驱动，
// 遥测、重试、补传均不得隐式改变阶段。
const (
	PhaseStandby   = "STANDBY"    // 待命
	PhaseRelocate  = "RELOCATE"   // 转场
	PhaseNetworkUp = "NETWORK_UP" // 建网
	PhaseSustain   = "SUSTAIN"    // 保障
	PhaseWithdraw  = "WITHDRAW"   // 撤收
)

// phases 给出允许的阶段顺序。只允许向后一阶段推进，不允许跳级或回退。
var phases = []string{
	PhaseStandby,
	PhaseRelocate,
	PhaseNetworkUp,
	PhaseSustain,
	PhaseWithdraw,
}

// PhaseRank 返回阶段在生命周期中的序号（STANDBY=0），未知阶段返回 -1。
func PhaseRank(phase string) int {
	for i, p := range phases {
		if p == phase {
			return i
		}
	}
	return -1
}

// ValidPhase 判断阶段是否为受控词汇。
func ValidPhase(phase string) bool { return PhaseRank(phase) != -1 }

// 改派触发类型（trigger_type），每类映射一个必须确认的岗位。
const (
	TriggerCoverageDrop = "COVERAGE_DROP"          // 覆盖下降
	TriggerAirspace     = "AIRSPACE_CONFLICT"      // 空域冲突
	TriggerPersonFound  = "PERSON_LOCATION_UPDATE" // 失联人员位置更新
	TriggerSecondary    = "SECONDARY_DISASTER"     // 次生灾害
)

// 人工指令类型。
const (
	ManualFreezeCapacity = "FREEZE_CAPACITY" // 冻结自动抢占：自动策略不得再抢占普通容量
	ManualResumeCapacity = "RESUME_CAPACITY" // 解除冻结
	ManualForce          = "FORCE"           // 强制指令：任何自动策略都不得覆盖
)

// 载荷事件类型（type）。
const (
	TypeAreaVersioned        = "area.versioned"
	TypeDroneRegistered      = "drone.registered"
	TypeDroneTelemetry       = "drone.telemetry"
	TypeDroneFuelBattery     = "drone.fuel_battery"
	TypeClearanceGranted     = "clearance.granted"
	TypePlanSigned           = "plan.signed"
	TypePlanActivated        = "plan.activated"
	TypeTaskPhaseChanged     = "task.phase_changed"
	TypeRouteUpdated         = "route.updated"
	TypeRoutePinned          = "route.pinned"
	TypeStationCoverage      = "station.coverage"
	TypeGroundTeamDemand     = "ground_team.demand"
	TypePrioritySet          = "rescue_priority.set"
	TypeRetaskProposed       = "retask.proposed"
	TypeRetaskConfirmed      = "retask.confirmed"
	TypeRetaskRejected       = "retask.rejected"
	TypeCallRequested        = "call.requested"
	TypeCallQueued           = "call.queued"
	TypeCallPreempted        = "call.preempted"
	TypeCallAdmitted         = "call.admitted"
	TypeCallEnded            = "call.ended"
	TypeCompensationReplayed = "call.compensation_replayed"
	TypeManualDirective      = "manual.directive"
	TypeReconResult          = "recon.result"
	TypePersonSighted        = "person.sighted"
	TypeSecondaryDisaster    = "disaster.secondary_observed"
	TypeAirspaceNotice       = "airspace.notice"
)

// RequiredRoleForTrigger 返回某类改派触发所要求的确认岗位。
func RequiredRoleForTrigger(trigger string) string {
	switch trigger {
	case TriggerCoverageDrop:
		return RoleNetworkOps
	case TriggerAirspace:
		return RoleAirspace
	case TriggerPersonFound:
		return RoleGroundTeam
	case TriggerSecondary:
		return RoleCommander
	}
	return ""
}

// ValidTrigger 判断改派触发类型是否受控。
func ValidTrigger(trigger string) bool { return RequiredRoleForTrigger(trigger) != "" }

// Envelope 是所有业务事实的统一信封。
type Envelope struct {
	SchemaVersion  string          `json:"schema_version"`
	EventID        string          `json:"event_id"`                // 全局幂等标识，重复摄入同一 ID 只生效一次
	SubjectRef     string          `json:"subject_ref"`             // 稳定引用编号（任务/区域/无人机/计划/通话…）
	Type           string          `json:"type"`                    // 受控事件类型
	OccurredAt     string          `json:"occurred_at"`             // 事件在现场发生的时间（ISO 8601 带偏移量）
	Source         string          `json:"source"`                  // 来源标识，序号只在同一来源内有意义
	SourceSequence int64           `json:"source_sequence"`         // 来源内单调连续递增，从 1 开始
	DeviceSerial   string          `json:"device_serial,omitempty"` // 相关设备序号（无人机/地面终端/基站）
	Operator       string          `json:"operator,omitempty"`      // 触发或签署该事件的岗位/人员稳定引用
	Payload        json.RawMessage `json:"payload"`
	PayloadDigest  string          `json:"payload_digest"` // "sha256:<hex>"，对规范化载荷计算
}

// ParseTime 解析带偏移量的 ISO 8601 时间；空串返回零值。
func ParseTime(value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, nil
	}
	return time.Parse(time.RFC3339, value)
}

// MustParseTime 供测试与可信内部数据使用。
func MustParseTime(value string) time.Time {
	t, err := ParseTime(value)
	if err != nil {
		panic(err)
	}
	return t
}

// CanonicalDigest 对任意 JSON 可序列化载荷计算规范化摘要：
// 先解码再以 map 形态重新编码，消除字段顺序与空白差异。
func CanonicalDigest(payload any) (json.RawMessage, string, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, "", err
	}
	var canonical any
	if err := json.Unmarshal(raw, &canonical); err != nil {
		return nil, "", err
	}
	ordered, err := json.Marshal(canonical)
	if err != nil {
		return nil, "", err
	}
	sum := sha256.Sum256(ordered)
	return json.RawMessage(ordered), "sha256:" + hex.EncodeToString(sum[:]), nil
}

// VerifyDigest 校验载荷摘要与信封声明一致（用于重放与补传场景）。
func VerifyDigest(raw json.RawMessage, digest string) bool {
	var canonical any
	if err := json.Unmarshal(raw, &canonical); err != nil {
		return false
	}
	ordered, err := json.Marshal(canonical)
	if err != nil {
		return false
	}
	sum := sha256.Sum256(ordered)
	return "sha256:"+hex.EncodeToString(sum[:]) == strings.TrimSpace(digest)
}
