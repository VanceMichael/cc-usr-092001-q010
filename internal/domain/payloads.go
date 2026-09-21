package domain

import (
	"encoding/json"
	"fmt"
)

// 以下载荷结构定义每种事件类型携带的业务字段。时间字段一律使用
// 带偏移量的 ISO 8601 字符串；标识字段一律为不含真实身份信息的稳定引用。

// AreaVersionedPayload 灾害区域版本。区域边界或风险要素每次变化都升版，
// 计划必须锚定某一具体版本。
type AreaVersionedPayload struct {
	AreaID      string `json:"area_id"`
	Version     int    `json:"version"`
	Name        string `json:"name,omitempty"`
	ChangedNote string `json:"changed_note,omitempty"` // 版本变化说明（受控引用/摘要）
	Supersedes  int    `json:"supersedes,omitempty"`   // 被替代的版本号
}

// DroneRegisteredPayload 无人机能力登记。
type DroneRegisteredPayload struct {
	DroneSerial       string `json:"drone_serial"`
	ModelRef          string `json:"model_ref"`
	ReconCapability   bool   `json:"recon_capability"`
	StationCapability bool   `json:"station_capability"` // 可承载空中基站
	MaxFlightMinutes  int    `json:"max_flight_minutes"`
	FuelKind          string `json:"fuel_kind"` // FUEL 燃油 / BATTERY 电池 / HYBRID
}

// DroneTelemetryPayload 无人机遥测。遥测只更新位置与姿态，绝不改变任务阶段。
type DroneTelemetryPayload struct {
	DroneSerial string  `json:"drone_serial"`
	Latitude    float64 `json:"latitude"`
	Longitude   float64 `json:"longitude"`
	AltitudeM   float64 `json:"altitude_m"`
	HeadingDeg  float64 `json:"heading_deg,omitempty"`
	PlanID      string  `json:"plan_id,omitempty"`      // 产生该遥测时现场执行的计划
	WaypointSeq int     `json:"waypoint_seq,omitempty"` // 当时航点序号
}

// DroneFuelBatteryPayload 油电状态。
type DroneFuelBatteryPayload struct {
	DroneSerial      string `json:"drone_serial"`
	FuelPercent      int    `json:"fuel_percent,omitempty"`
	BatteryPercent   int    `json:"battery_percent,omitempty"`
	RemainingMinutes int    `json:"remaining_minutes"`
}

// ClearanceGrantedPayload 起飞/空域许可。
type ClearanceGrantedPayload struct {
	ClearanceID string `json:"clearance_id"`
	DroneSerial string `json:"drone_serial"`
	AirspaceRef string `json:"airspace_ref"`
	ValidFrom   string `json:"valid_from"`
	ValidUntil  string `json:"valid_until"`
	GrantedBy   string `json:"granted_by"` // 授权岗位稳定引用
}

// PlanSignedPayload 签署计划。签署后计划成为现场可离线使用的事实版本；
// 中心重启后，现场继续使用最后一份已签署（且已激活）的计划。
type PlanSignedPayload struct {
	PlanID       string   `json:"plan_id"`
	TaskRef      string   `json:"task_ref"`
	AreaID       string   `json:"area_id"`
	AreaVersion  int      `json:"area_version"`
	DroneSerials []string `json:"drone_serials"`
	RouteRef     string   `json:"route_ref"`
	StationRef   string   `json:"station_ref"`
	RevisionOf   string   `json:"revision_of,omitempty"` // 若为改牌后重签，指向前序计划
	NotesDigest  string   `json:"notes_digest,omitempty"`
}

// PlanActivatedPayload 计划激活：现场以此计划为准执行。
type PlanActivatedPayload struct {
	PlanID      string `json:"plan_id"`
	TaskRef     string `json:"task_ref"`
	ActivatedBy string `json:"activated_by"`
}

// TaskPhaseChangedPayload 任务阶段变化。每一次变化都带设备序号与发生时间，
// 是唯一允许改变飞行阶段的事件。
type TaskPhaseChangedPayload struct {
	TaskRef     string `json:"task_ref"`
	FromPhase   string `json:"from_phase"`
	ToPhase     string `json:"to_phase"`
	DroneSerial string `json:"drone_serial"`
	Reason      string `json:"reason"`
	PlanID      string `json:"plan_id,omitempty"`
}

// RouteUpdatedPayload 航线更新（计划内航点）。
type RouteUpdatedPayload struct {
	RouteRef  string     `json:"route_ref"`
	PlanID    string     `json:"plan_id,omitempty"`
	Waypoints []Waypoint `json:"waypoints"`
	ChangedBy string     `json:"changed_by"`
}

// RoutePinnedPayload 航线钉死：待确认改派期间，自动策略不得变更航线。
type RoutePinnedPayload struct {
	RouteRef string `json:"route_ref"`
	RetaskID string `json:"retask_id"`
	PinnedBy string `json:"pinned_by"`
}

// Waypoint 航点。
type Waypoint struct {
	Seq       int     `json:"seq"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	AltitudeM float64 `json:"altitude_m"`
	ETA       string  `json:"eta,omitempty"`
}

// StationCoveragePayload 空中基站覆盖与容量状态。
type StationCoveragePayload struct {
	StationRef  string  `json:"station_ref"`
	DroneSerial string  `json:"drone_serial"`
	Latitude    float64 `json:"latitude"`
	Longitude   float64 `json:"longitude"`
	RadiusM     float64 `json:"radius_m"`
	Capacity    int     `json:"capacity"` // 当前总容量（信道数）
	Used        int     `json:"used"`     // 已占用（含高优先级）
	Active      bool    `json:"active"`
}

// GroundTeamDemandPayload 地面队伍提出的通信/侦察需求。
type GroundTeamDemandPayload struct {
	DemandID  string  `json:"demand_id"`
	TeamRef   string  `json:"team_ref"`
	Kind      string  `json:"kind"`     // COMMS 通信 / RECON 侦察
	Priority  int     `json:"priority"` // 救援优先级，1 最高
	Detail    string  `json:"detail,omitempty"`
	Latitude  float64 `json:"latitude,omitempty"`
	Longitude float64 `json:"longitude,omitempty"`
}

// PrioritySetPayload 救援优先级设定/调整。
type PrioritySetPayload struct {
	SubjectRef string `json:"subject_ref"` // 需求点/队伍/区域的稳定引用
	Priority   int    `json:"priority"`
	SetBy      string `json:"set_by"`
	Reason     string `json:"reason,omitempty"`
}

// RetaskProposedPayload 受控改派提议。覆盖下降、空域冲突、失联人员位置更新、
// 次生灾害四类情形生成；必须等待对应岗位确认才生效，同时钉死航线。
type RetaskProposedPayload struct {
	RetaskID       string `json:"retask_id"`
	TaskRef        string `json:"task_ref"`
	TriggerType    string `json:"trigger_type"`
	RequiredRole   string `json:"required_role"` // 由触发类型映射
	Summary        string `json:"summary"`
	ProposedPlanID string `json:"proposed_plan_id"` // 待确认的新计划草案
	CurrentPlanID  string `json:"current_plan_id"`
	RouteRef       string `json:"route_ref"`
	EvidenceDigest string `json:"evidence_digest,omitempty"` // 触发证据摘要
}

// RetaskConfirmedPayload 岗位确认改派，新计划随即生效。
type RetaskConfirmedPayload struct {
	RetaskID    string `json:"retask_id"`
	ConfirmedBy string `json:"confirmed_by"` // 必须等于 required_role
	Role        string `json:"role"`
	PlanID      string `json:"plan_id,omitempty"` // 确认时选定的重签计划；为空则采用提案草案
}

// RetaskRejectedPayload 岗位拒绝改派，解除航线钉死，维持原计划。
type RetaskRejectedPayload struct {
	RetaskID   string `json:"retask_id"`
	RejectedBy string `json:"rejected_by"`
	Role       string `json:"role"`
	Reason     string `json:"reason,omitempty"`
}

// CallRequestedPayload 应急通话请求。
type CallRequestedPayload struct {
	CallID     string `json:"call_id"`
	StationRef string `json:"station_ref"`
	TeamRef    string `json:"team_ref,omitempty"`
	Priority   int    `json:"priority"` // 1 为高优先级（可抢占），数字越大越低
	Subject    string `json:"subject,omitempty"`
}

// CallQueuedPayload 通话进入等待队列（普通拥塞排队、人工冻结或无可抢占对象）。
type CallQueuedPayload struct {
	CallID     string `json:"call_id"`
	StationRef string `json:"station_ref"`
	Reason     string `json:"reason"` // CONGESTED / FROZEN / NO_VICTIM
}

// CallPreemptedPayload 高优先级通话抢占普通容量的裁决记录。
// 被抢占的普通通话进入补偿队列，必须留下补偿条目，稍后自动回放。
type CallPreemptedPayload struct {
	StationRef      string `json:"station_ref"`
	PreemptedCallID string `json:"preempted_call_id"` // 被抢占的普通通话
	PriorityCallID  string `json:"priority_call_id"`  // 获得容量的高优先级通话
	CompensationID  string `json:"compensation_id"`   // 补偿队列条目
	DecidedBy       string `json:"decided_by"`        // AUTOMATIC 或岗位引用
}

// CallAdmittedPayload 通话获得容量（含被抢占后补偿回放成功）。
type CallAdmittedPayload struct {
	CallID            string `json:"call_id"`
	StationRef        string `json:"station_ref"`
	ViaCompensationID string `json:"via_compensation_id,omitempty"`
	Forced            bool   `json:"forced,omitempty"` // 经人工 FORCE 指令接入
}

// CallEndedPayload 通话结束，释放容量。
type CallEndedPayload struct {
	CallID string `json:"call_id"`
	Reason string `json:"reason,omitempty"`
}

// CompensationReplayedPayload 补偿队列回放：先前被抢占的普通通话重新接通。
type CompensationReplayedPayload struct {
	CompensationID string `json:"compensation_id"`
	CallID         string `json:"call_id"`
	StationRef     string `json:"station_ref"`
}

// ManualDirectivePayload 人工强制指令。FREEZE_CAPACITY 冻结自动抢占，
// FORCE 携带必须被执行且自动策略不得覆盖的决定。
type ManualDirectivePayload struct {
	DirectiveID string `json:"directive_id"`
	Kind        string `json:"kind"` // FREEZE_CAPACITY / RESUME_CAPACITY / FORCE
	IssuedBy    string `json:"issued_by"`
	Role        string `json:"role"`
	Scope       string `json:"scope,omitempty"`    // 作用对象（任务/基站/航线）
	Decision    string `json:"decision,omitempty"` // FORCE 的具体决定
	Reason      string `json:"reason,omitempty"`
}

// ReconResultPayload 侦察成果。事后输入某成果可还原当时位置与责任链。
type ReconResultPayload struct {
	ReconID       string  `json:"recon_id"`
	DroneSerial   string  `json:"drone_serial"`
	PlanID        string  `json:"plan_id"`
	Latitude      float64 `json:"latitude"`
	Longitude     float64 `json:"longitude"`
	FindingRef    string  `json:"finding_ref"` // 受控引用，不存真实身份信息
	FindingDigest string  `json:"finding_digest"`
}

// PersonSightedPayload 失联人员位置更新（地面队伍/侦察上报）。
type PersonSightedPayload struct {
	SightingID  string  `json:"sighting_id"`
	TaskRef     string  `json:"task_ref"`
	TeamRef     string  `json:"team_ref"`
	Latitude    float64 `json:"latitude"`
	Longitude   float64 `json:"longitude"`
	LocationRef string  `json:"location_ref,omitempty"` // 受控地点引用
}

// SecondaryDisasterPayload 次生灾害出现的观测事实。
type SecondaryDisasterPayload struct {
	ObservationID  string `json:"observation_id"`
	TaskRef        string `json:"task_ref"`
	Kind           string `json:"kind"` // 二次滑坡/堰塞湖/…
	AreaID         string `json:"area_id"`
	NewAreaVersion int    `json:"new_area_version"` // 观测后发布的区域新版本（可为 0）
	LocationRef    string `json:"location_ref,omitempty"`
}

// AirspaceNoticePayload 空域冲突通告。
type AirspaceNoticePayload struct {
	NoticeID    string `json:"notice_id"`
	TaskRef     string `json:"task_ref"`
	AirspaceRef string `json:"airspace_ref"`
	Kind        string `json:"kind"` // RESTRICTION / CONFLICT / CLOSURE
	DetailRef   string `json:"detail_ref,omitempty"`
}

// payloadFactories 按事件类型返回一个空的强类型载荷，供解码与校验使用。
var payloadFactories = map[string]func() any{
	TypeAreaVersioned:        func() any { return &AreaVersionedPayload{} },
	TypeDroneRegistered:      func() any { return &DroneRegisteredPayload{} },
	TypeDroneTelemetry:       func() any { return &DroneTelemetryPayload{} },
	TypeDroneFuelBattery:     func() any { return &DroneFuelBatteryPayload{} },
	TypeClearanceGranted:     func() any { return &ClearanceGrantedPayload{} },
	TypePlanSigned:           func() any { return &PlanSignedPayload{} },
	TypePlanActivated:        func() any { return &PlanActivatedPayload{} },
	TypeTaskPhaseChanged:     func() any { return &TaskPhaseChangedPayload{} },
	TypeRouteUpdated:         func() any { return &RouteUpdatedPayload{} },
	TypeRoutePinned:          func() any { return &RoutePinnedPayload{} },
	TypeStationCoverage:      func() any { return &StationCoveragePayload{} },
	TypeGroundTeamDemand:     func() any { return &GroundTeamDemandPayload{} },
	TypePrioritySet:          func() any { return &PrioritySetPayload{} },
	TypeRetaskProposed:       func() any { return &RetaskProposedPayload{} },
	TypeRetaskConfirmed:      func() any { return &RetaskConfirmedPayload{} },
	TypeRetaskRejected:       func() any { return &RetaskRejectedPayload{} },
	TypeCallRequested:        func() any { return &CallRequestedPayload{} },
	TypeCallQueued:           func() any { return &CallQueuedPayload{} },
	TypeCallPreempted:        func() any { return &CallPreemptedPayload{} },
	TypeCallAdmitted:         func() any { return &CallAdmittedPayload{} },
	TypeCallEnded:            func() any { return &CallEndedPayload{} },
	TypeCompensationReplayed: func() any { return &CompensationReplayedPayload{} },
	TypeManualDirective:      func() any { return &ManualDirectivePayload{} },
	TypeReconResult:          func() any { return &ReconResultPayload{} },
	TypePersonSighted:        func() any { return &PersonSightedPayload{} },
	TypeSecondaryDisaster:    func() any { return &SecondaryDisasterPayload{} },
	TypeAirspaceNotice:       func() any { return &AirspaceNoticePayload{} },
}

// DecodePayload 按事件类型把载荷解码为强类型结构；未知类型返回错误。
func DecodePayload(eventType string, raw json.RawMessage) (any, error) {
	factory, ok := payloadFactories[eventType]
	if !ok {
		return nil, fmt.Errorf("未知事件类型 %q", eventType)
	}
	payload := factory()
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, payload); err != nil {
			return nil, fmt.Errorf("解码 %s 载荷失败: %w", eventType, err)
		}
	}
	return payload, nil
}

// KnownEventType 判断事件类型是否已注册。
func KnownEventType(eventType string) bool {
	_, ok := payloadFactories[eventType]
	return ok
}
