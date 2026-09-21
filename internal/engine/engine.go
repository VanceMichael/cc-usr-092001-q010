// Package engine 实现任务控制器的核心规则：
//
//   - 事件是唯一事实来源。命令经校验后追加到仅追加日志，状态由日志重放得到。
//   - 每个来源的序号必须连续：缺口事件先缓冲，离线补传按序释放；重复 event_id
//     幂等忽略；序号回退拒绝——这些路径都不会产生任何新的阶段事件。
//   - 任务阶段只能由 task.phase_changed 显式推进，且必须相邻、带来源事实，
//     遥测/重试/补传均无法隐式改变飞行阶段。
//   - 改派提议后航线钉死，必须由触发类型对应的岗位确认才生效；人工指令优先于
//     一切自动策略。
//   - 高优先级通话可抢占普通容量，但必须留下补偿队列条目，容量恢复后按序回放。
//   - 所有派生事件（裁决、回放、改派提案、计划切换）的标识由父事件确定性导出，
//     中心重试不会制造重复事实。
package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"example.com/batch-092001-q010/internal/domain"
	"example.com/batch-092001-q010/internal/store"
)

// RejectCode 是业务拒绝的受控原因码，便于调用方区分处理。
type RejectCode string

const (
	RejectValidation       RejectCode = "VALIDATION_FAILED"
	RejectStaleSequence    RejectCode = "STALE_SEQUENCE"
	RejectSequenceConflict RejectCode = "SEQUENCE_CONFLICT"
	RejectRule             RejectCode = "RULE_VIOLATION"
)

// RejectError 携带原因码与可读说明。
type RejectError struct {
	Code   RejectCode
	Reason string
}

func (e *RejectError) Error() string { return fmt.Sprintf("%s: %s", e.Code, e.Reason) }

func reject(code RejectCode, format string, args ...any) error {
	return &RejectError{Code: code, Reason: fmt.Sprintf(format, args...)}
}

// Result 描述一次摄入的结果。
type Result struct {
	EventID   string          `json:"event_id"`
	Offset    int64           `json:"offset"`
	Duplicate bool            `json:"duplicate,omitempty"` // 同一 event_id 已生效或已缓冲
	Buffered  bool            `json:"buffered,omitempty"`  // 因序号缺口暂存，尚未生效
	Derived   []DerivedEffect `json:"derived,omitempty"`   // 本次释放连锁产生的系统事件
}

// DerivedEffect 记录一次自动派生的事实。
type DerivedEffect struct {
	EventID string `json:"event_id"`
	Type    string `json:"type"`
	Summary string `json:"summary"`
}

// Engine 是控制器。所有方法均串行化，可安全并发使用。
type Engine struct {
	mu          sync.Mutex
	log         store.Log
	state       *State
	seen        map[string]bool                      // 已生效 event_id
	bufferedIDs map[string]bool                      // 已缓冲 event_id
	pending     map[string]map[int64]domain.Envelope // source -> seq -> 事件
	nextSeq     map[string]int64                     // source -> 下一个期望序号
	derivedSeen map[string]bool                      // 派生事件幂等表（与 seen 同源记录）
}

// New 打开引擎并重放日志重建全部状态。
func New(log store.Log) (*Engine, error) {
	e := &Engine{
		log:         log,
		state:       NewState(),
		seen:        map[string]bool{},
		bufferedIDs: map[string]bool{},
		pending:     map[string]map[int64]domain.Envelope{},
		nextSeq:     map[string]int64{},
		derivedSeen: map[string]bool{},
	}
	if err := e.recover(); err != nil {
		return nil, err
	}
	return e, nil
}

// recover 从日志重放：落盘事件均为"已按序释放"的事实，直接投影，
// 并恢复各来源的序号水位，使重启后幂等与缓冲规则继续有效。
func (e *Engine) recover() error {
	maxSeq := map[string]int64{}
	err := e.log.Replay(func(rec store.Record) error {
		env := rec.Envelope
		if env.Source != "" && env.SourceSequence > maxSeq[env.Source] {
			maxSeq[env.Source] = env.SourceSequence
		}
		e.seen[env.EventID] = true
		e.state.Apply(env)
		return nil
	})
	if err != nil {
		return err
	}
	for source, seq := range maxSeq {
		e.nextSeq[source] = seq + 1
	}
	return nil
}

// Ingest 摄入一个事件信封：校验、缓冲或追加生效，并执行连锁自动化。
func (e *Engine) Ingest(env domain.Envelope) (*Result, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if err := validateEnvelope(env); err != nil {
		return nil, err
	}

	// 幂等：同一 event_id 的重复遥测、中心重试，直接返回首次结果，不产生任何副作用。
	if e.seen[env.EventID] {
		return &Result{EventID: env.EventID, Duplicate: true}, nil
	}
	if e.bufferedIDs[env.EventID] {
		return &Result{EventID: env.EventID, Duplicate: true, Buffered: true}, nil
	}

	// 新来源的首个事件序号必须为 1；已见来源从其水位继续。
	expected, known := e.nextSeq[env.Source]
	if !known {
		expected = 1
	}
	if env.SourceSequence < expected {
		// 迟到的旧序号：它所代表的事实已被同序号事件覆盖，拒绝而非再投影一次。
		return nil, reject(RejectStaleSequence,
			"来源 %s 的序号 %d 已过期（期望 %d），迟到补传不得覆盖既有事实",
			env.Source, env.SourceSequence, expected)
	}
	if env.SourceSequence > expected {
		// 序号缺口：先缓冲，等待缺口事件补传后按序释放。
		if e.pending[env.Source] == nil {
			e.pending[env.Source] = map[int64]domain.Envelope{}
		}
		if occupied, ok := e.pending[env.Source][env.SourceSequence]; ok && occupied.EventID != env.EventID {
			return nil, reject(RejectSequenceConflict,
				"来源 %s 序号 %d 已被事件 %s 占用", env.Source, env.SourceSequence, occupied.EventID)
		}
		e.pending[env.Source][env.SourceSequence] = env
		e.bufferedIDs[env.EventID] = true
		return &Result{EventID: env.EventID, Buffered: true}, nil
	}

	// 序号恰好衔接：立即释放，并连带释放后续已缓冲链。
	var released []domain.Envelope
	released = append(released, env)
	for seq := expected + 1; ; seq++ {
		next, ok := e.pending[env.Source][seq]
		if !ok {
			break
		}
		delete(e.pending[env.Source], seq)
		delete(e.bufferedIDs, next.EventID)
		released = append(released, next)
	}

	var derived []DerivedEffect
	for _, current := range released {
		// 业务规则校验只在"按序释放"时刻进行：缺口内的事件未参与状态，
		// 因此离线补传不会与尚未到达的事实相互误判。
		if err := e.validateBusiness(current); err != nil {
			// 校验失败的事件不放回缓冲：它序号合法但内容违规，明确拒绝。
			return nil, err
		}
		offset, err := e.log.Append(current)
		if err != nil {
			return nil, err
		}
		e.seen[current.EventID] = true
		e.nextSeq[current.Source] = current.SourceSequence + 1
		e.state.Apply(current)
		derived = append(derived, e.automate(current)...)
		_ = offset
	}
	last := released[len(released)-1]
	return &Result{EventID: last.EventID, Offset: e.log.Len() - 1, Derived: derived}, nil
}

// validateEnvelope 校验信封自洽性（与状态无关的部分）。
func validateEnvelope(env domain.Envelope) error {
	if env.SchemaVersion != "" && env.SchemaVersion != domain.SchemaVersion {
		return reject(RejectValidation, "schema_version %q 不受支持（当前 %q）",
			env.SchemaVersion, domain.SchemaVersion)
	}
	if env.EventID == "" {
		return reject(RejectValidation, "event_id 不能为空")
	}
	if env.SubjectRef == "" {
		return reject(RejectValidation, "subject_ref 不能为空")
	}
	if !domain.KnownEventType(env.Type) {
		return reject(RejectValidation, "未知事件类型 %q", env.Type)
	}
	if env.Source == "" {
		return reject(RejectValidation, "source 不能为空")
	}
	if strings.HasPrefix(env.Source, "controller:") {
		return reject(RejectValidation, "来源前缀 controller: 为系统裁决保留，外部事件不得使用")
	}
	if env.SourceSequence < 1 {
		return reject(RejectValidation, "source_sequence 必须从 1 开始递增")
	}
	at, err := domain.ParseTime(env.OccurredAt)
	if err != nil {
		return reject(RejectValidation, "occurred_at 不是合法的 ISO 8601 时间: %v", err)
	}
	if at.IsZero() {
		return reject(RejectValidation, "occurred_at 不能为空（必须保留现场发生时间）")
	}
	if len(env.Payload) == 0 {
		return reject(RejectValidation, "payload 不能为空")
	}
	// 飞行阶段的每次变化都必须带来源设备序号：信封与载荷双重约束。
	if env.Type == domain.TypeTaskPhaseChanged && env.DeviceSerial == "" {
		return reject(RejectValidation, "阶段变化事件必须在信封中携带 device_serial")
	}
	if env.PayloadDigest != "" && !domain.VerifyDigest(env.Payload, env.PayloadDigest) {
		return reject(RejectValidation, "payload_digest 与载荷不一致，事件可能被篡改")
	}
	if _, err := domain.DecodePayload(env.Type, env.Payload); err != nil {
		return reject(RejectValidation, "%v", err)
	}
	return nil
}

// mustPayload 把已校验的载荷解析为强类型值。
func mustPayload(env domain.Envelope) any {
	p, _ := domain.DecodePayload(env.Type, env.Payload)
	return p
}

// validateBusiness 执行与当前状态相关的业务规则。
func (e *Engine) validateBusiness(env domain.Envelope) error {
	at, _ := domain.ParseTime(env.OccurredAt)
	switch p := mustPayload(env).(type) {
	case *domain.AreaVersionedPayload:
		if p.AreaID == "" || p.Version < 1 {
			return reject(RejectRule, "区域标识为空或版本号非法")
		}
		if existing, ok := e.state.Areas[p.AreaID]; ok && p.Version <= existing.Latest.Version {
			return reject(RejectRule, "区域 %s 已存在版本 %d，不得用版本 %d 回退",
				p.AreaID, existing.Latest.Version, p.Version)
		}

	case *domain.DroneRegisteredPayload:
		if p.DroneSerial == "" {
			return reject(RejectRule, "无人机设备序号不能为空")
		}
		if p.MaxFlightMinutes < 0 {
			return reject(RejectRule, "最大续航时间不能为负")
		}

	case *domain.DroneTelemetryPayload:
		if p.DroneSerial == "" {
			return reject(RejectRule, "遥测必须带设备序号")
		}
		if _, ok := e.state.Drones[p.DroneSerial]; !ok {
			return reject(RejectRule, "无人机 %s 尚未登记能力，遥测不能先于登记生效", p.DroneSerial)
		}
		// 注意：这里刻意不触碰任何任务阶段。

	case *domain.DroneFuelBatteryPayload:
		if _, ok := e.state.Drones[p.DroneSerial]; !ok {
			return reject(RejectRule, "无人机 %s 尚未登记，油电状态不能生效", p.DroneSerial)
		}
		if p.FuelPercent < 0 || p.FuelPercent > 100 || p.BatteryPercent < 0 || p.BatteryPercent > 100 {
			return reject(RejectRule, "油电百分比必须在 0-100 之间")
		}

	case *domain.ClearanceGrantedPayload:
		if p.ClearanceID == "" || p.DroneSerial == "" || p.GrantedBy == "" {
			return reject(RejectRule, "许可标识、无人机序号与授权岗位均不能为空")
		}
		if _, ok := e.state.Drones[p.DroneSerial]; !ok {
			return reject(RejectRule, "无人机 %s 尚未登记，许可不能生效", p.DroneSerial)
		}
		from, err := domain.ParseTime(p.ValidFrom)
		if err != nil || from.IsZero() {
			return reject(RejectRule, "许可生效时间非法")
		}
		until, err := domain.ParseTime(p.ValidUntil)
		if err != nil || !until.After(from) {
			return reject(RejectRule, "许可失效时间必须晚于生效时间")
		}

	case *domain.PlanSignedPayload:
		if p.PlanID == "" || p.TaskRef == "" || p.AreaID == "" {
			return reject(RejectRule, "计划标识、任务引用与区域标识不能为空")
		}
		if _, ok := e.state.Areas[p.AreaID]; !ok {
			return reject(RejectRule, "计划锚定的区域 %s 不存在", p.AreaID)
		}
		area := e.state.Areas[p.AreaID]
		if p.AreaVersion != area.Latest.Version {
			return reject(RejectRule, "计划必须锚定当前区域版本 %d（提交为 %d）",
				area.Latest.Version, p.AreaVersion)
		}
		if len(p.DroneSerials) == 0 {
			return reject(RejectRule, "签署计划必须至少包含一架无人机")
		}
		for _, serial := range p.DroneSerials {
			if _, ok := e.state.Drones[serial]; !ok {
				return reject(RejectRule, "计划中的无人机 %s 尚未登记", serial)
			}
		}
		if p.RouteRef == "" || p.StationRef == "" {
			return reject(RejectRule, "签署计划必须包含航线与空中基站引用")
		}

	case *domain.PlanActivatedPayload:
		plan, ok := e.state.Plans[p.PlanID]
		if !ok {
			return reject(RejectRule, "计划 %s 尚未签署，不能激活", p.PlanID)
		}
		if plan.TaskRef != p.TaskRef {
			return reject(RejectRule, "计划属于任务 %s，不能作为任务 %s 激活", plan.TaskRef, p.TaskRef)
		}

	case *domain.TaskPhaseChangedPayload:
		if err := e.validatePhaseChange(p, at, env); err != nil {
			return err
		}

	case *domain.RouteUpdatedPayload:
		if p.RouteRef == "" || len(p.Waypoints) == 0 {
			return reject(RejectRule, "航线引用与航点不能为空")
		}
		if pinned, retaskID := e.state.RoutePinned(p.RouteRef); pinned && !e.humanOverride(env, "UNPIN_ROUTE", p.RouteRef, at) {
			return reject(RejectRule, "航线 %s 因待确认改派 %s 已钉死，自动策略不得变更（需岗位强制指令）",
				p.RouteRef, retaskID)
		}

	case *domain.RoutePinnedPayload:
		if _, ok := e.state.Retasks[p.RetaskID]; !ok {
			return reject(RejectRule, "钉航线所引用的改派 %s 不存在", p.RetaskID)
		}

	case *domain.StationCoveragePayload:
		if p.StationRef == "" {
			return reject(RejectRule, "基站引用不能为空")
		}
		if p.Capacity < 0 || p.Used < 0 || p.Used > p.Capacity {
			return reject(RejectRule, "基站容量/占用非法：capacity=%d used=%d", p.Capacity, p.Used)
		}

	case *domain.GroundTeamDemandPayload:
		if p.DemandID == "" || p.TeamRef == "" {
			return reject(RejectRule, "需求标识与队伍引用不能为空")
		}
		if p.Kind != "COMMS" && p.Kind != "RECON" {
			return reject(RejectRule, "需求类型必须为 COMMS 或 RECON")
		}
		if p.Priority < 1 {
			return reject(RejectRule, "救援优先级必须从 1 开始")
		}

	case *domain.PrioritySetPayload:
		if p.SubjectRef == "" || p.SetBy == "" || p.Priority < 1 {
			return reject(RejectRule, "优先级对象、设定岗位与优先级值均不能为空/非法")
		}

	case *domain.RetaskProposedPayload:
		if err := e.validateRetaskProposal(p); err != nil {
			return err
		}

	case *domain.RetaskConfirmedPayload:
		r, ok := e.state.Retasks[p.RetaskID]
		if !ok {
			return reject(RejectRule, "改派 %s 不存在", p.RetaskID)
		}
		if r.Status != RetaskProposed {
			return reject(RejectRule, "改派 %s 已处理（%s），不能重复确认", p.RetaskID, r.Status)
		}
		if p.Role != r.RequiredRole {
			return reject(RejectRule, "改派 %s 要求 %s 岗位确认，%s 无权确认",
				p.RetaskID, r.RequiredRole, p.Role)
		}
		if p.ConfirmedBy == "" {
			return reject(RejectRule, "确认必须留下责任岗位引用")
		}
		chosen := p.PlanID
		if chosen == "" {
			chosen = r.ProposedPlanID
		}
		plan, ok := e.state.Plans[chosen]
		if !ok {
			return reject(RejectRule, "确认改派所采用的计划 %s 尚未签署", chosen)
		}
		if plan.TaskRef != r.TaskRef {
			return reject(RejectRule, "计划 %s 不属于改派任务 %s", chosen, r.TaskRef)
		}

	case *domain.RetaskRejectedPayload:
		r, ok := e.state.Retasks[p.RetaskID]
		if !ok {
			return reject(RejectRule, "改派 %s 不存在", p.RetaskID)
		}
		if r.Status != RetaskProposed {
			return reject(RejectRule, "改派 %s 已处理（%s），不能重复拒绝", p.RetaskID, r.Status)
		}
		if p.Role != r.RequiredRole {
			return reject(RejectRule, "改派 %s 只能由 %s 岗位拒绝", p.RetaskID, r.RequiredRole)
		}

	case *domain.CallRequestedPayload:
		if p.CallID == "" || p.StationRef == "" {
			return reject(RejectRule, "通话标识与基站引用不能为空")
		}
		if _, ok := e.state.Stations[p.StationRef]; !ok {
			return reject(RejectRule, "基站 %s 尚未上报，通话请求不能裁决", p.StationRef)
		}
		if _, dup := e.state.Calls[p.CallID]; dup {
			return reject(RejectRule, "通话 %s 已存在，重复请求必须复用同一 event_id", p.CallID)
		}
		if p.Priority < 1 {
			return reject(RejectRule, "通话优先级必须从 1 开始")
		}

	case *domain.ManualDirectivePayload:
		if p.DirectiveID == "" || p.IssuedBy == "" || p.Role == "" {
			return reject(RejectRule, "人工指令必须有标识、下达人与岗位")
		}
		if p.Role == domain.RoleSystem {
			return reject(RejectRule, "人工指令不能由 SYSTEM 来源下达")
		}
		switch p.Kind {
		case domain.ManualFreezeCapacity, domain.ManualResumeCapacity:
			if p.Scope == "" {
				return reject(RejectRule, "冻结/恢复指令必须声明作用域（* 或 STATION:<ref>）")
			}
		case domain.ManualForce:
			if strings.TrimSpace(p.Decision) == "" {
				return reject(RejectRule, "FORCE 指令必须携带具体决定（decision）")
			}
		default:
			return reject(RejectRule, "未知人工指令类型 %q", p.Kind)
		}

	case *domain.ReconResultPayload:
		if p.ReconID == "" || p.DroneSerial == "" || p.FindingRef == "" || p.FindingDigest == "" {
			return reject(RejectRule, "侦察成果必须包含成果编号、设备序号、受控引用与摘要")
		}
		if _, ok := e.state.Drones[p.DroneSerial]; !ok {
			return reject(RejectRule, "侦察成果来自未登记无人机 %s", p.DroneSerial)
		}
		if p.PlanID != "" {
			if _, ok := e.state.Plans[p.PlanID]; !ok {
				return reject(RejectRule, "侦察成果锚定的计划 %s 不存在", p.PlanID)
			}
		}

	case *domain.PersonSightedPayload:
		if p.SightingID == "" || p.TaskRef == "" || p.TeamRef == "" {
			return reject(RejectRule, "人员位置更新必须包含观测编号、任务引用与上报队伍")
		}
		if e.state.ActivePlan(p.TaskRef) == nil {
			return reject(RejectRule, "任务 %s 没有激活中的计划，人员位置更新无法触发改派", p.TaskRef)
		}

	case *domain.SecondaryDisasterPayload:
		if p.ObservationID == "" || p.TaskRef == "" || p.Kind == "" || p.AreaID == "" {
			return reject(RejectRule, "次生灾害观测必须包含观测编号、任务、灾害类型与区域")
		}
		if _, ok := e.state.Areas[p.AreaID]; !ok {
			return reject(RejectRule, "次生灾害引用的区域 %s 不存在", p.AreaID)
		}
		if e.state.ActivePlan(p.TaskRef) == nil {
			return reject(RejectRule, "任务 %s 没有激活中的计划，次生灾害无法触发改派", p.TaskRef)
		}

	case *domain.AirspaceNoticePayload:
		if p.NoticeID == "" || p.TaskRef == "" || p.AirspaceRef == "" || p.Kind == "" {
			return reject(RejectRule, "空域通告必须包含通告编号、任务、空域引用与类型")
		}
		if e.state.ActivePlan(p.TaskRef) == nil {
			return reject(RejectRule, "任务 %s 没有激活中的计划，空域通告无法触发改派", p.TaskRef)
		}

	// 以下三类是控制器自身的派生事件，外部不得直接写入。
	case *domain.CallQueuedPayload, *domain.CallPreemptedPayload,
		*domain.CallAdmittedPayload, *domain.CompensationReplayedPayload:
		return reject(RejectRule, "事件类型 %s 由控制器裁决产生，不接受外部直接提交", env.Type)
	}
	return nil
}

// validatePhaseChange 集中处理阶段状态机。
func (e *Engine) validatePhaseChange(p *domain.TaskPhaseChangedPayload, at time.Time, env domain.Envelope) error {
	if p.TaskRef == "" {
		return reject(RejectRule, "阶段变化必须带任务引用")
	}
	if p.DroneSerial == "" {
		return reject(RejectRule, "阶段变化必须带设备序号，禁止无设备来源的阶段推进")
	}
	current := e.state.TaskPhase(p.TaskRef)
	if p.FromPhase != current {
		return reject(RejectRule, "任务 %s 当前阶段为 %s，from_phase=%s 与事实不符（补传/重试不得改写阶段）",
			p.TaskRef, current, p.FromPhase)
	}
	fromRank := domain.PhaseRank(p.FromPhase)
	toRank := domain.PhaseRank(p.ToPhase)
	if fromRank < 0 || toRank < 0 {
		return reject(RejectRule, "阶段词汇非法：%s -> %s", p.FromPhase, p.ToPhase)
	}
	if toRank != fromRank+1 {
		return reject(RejectRule, "阶段只能逐阶段推进，禁止从 %s 跳至 %s", p.FromPhase, p.ToPhase)
	}
	// 从待命转出进入实际飞行：必须持有效许可，或有人工 FORCE 豁免。
	if p.ToPhase == domain.PhaseRelocate {
		if !e.state.ClearanceValidAt(p.DroneSerial, at) && !e.state.OverrideClearance(p.TaskRef, at) {
			return reject(RejectRule, "无人机 %s 在 %s 无有效起飞许可，且无人工强制豁免",
				p.DroneSerial, env.OccurredAt)
		}
	}
	// 进入飞行必须有现场可执行的已签署计划；中心失联期间使用最后一份已激活计划。
	if toRank >= domain.PhaseRank(domain.PhaseRelocate) {
		plan := e.state.ActivePlan(p.TaskRef)
		if plan == nil && p.PlanID == "" {
			return reject(RejectRule, "任务 %s 没有已激活的签署计划，阶段不得推进", p.TaskRef)
		}
		if p.PlanID != "" {
			if _, ok := e.state.Plans[p.PlanID]; !ok {
				return reject(RejectRule, "阶段事件引用的计划 %s 尚未签署", p.PlanID)
			}
		}
	}
	return nil
}

func (e *Engine) validateRetaskProposal(p *domain.RetaskProposedPayload) error {
	if p.RetaskID == "" || p.TaskRef == "" {
		return reject(RejectRule, "改派标识与任务引用不能为空")
	}
	if !domain.ValidTrigger(p.TriggerType) {
		return reject(RejectRule, "改派触发类型 %q 不受控", p.TriggerType)
	}
	required := domain.RequiredRoleForTrigger(p.TriggerType)
	if p.RequiredRole != "" && p.RequiredRole != required {
		return reject(RejectRule, "触发类型 %s 的确认岗位必须是 %s", p.TriggerType, required)
	}
	current := e.state.ActivePlan(p.TaskRef)
	if current == nil {
		return reject(RejectRule, "任务 %s 没有激活中的计划，无法发起改派", p.TaskRef)
	}
	if p.CurrentPlanID != "" && p.CurrentPlanID != current.PlanID {
		return reject(RejectRule, "改派记录的当前计划 %s 与激活计划 %s 不符",
			p.CurrentPlanID, current.PlanID)
	}
	draft, ok := e.state.Plans[p.ProposedPlanID]
	if !ok {
		return reject(RejectRule, "改派草案计划 %s 尚未签署", p.ProposedPlanID)
	}
	if draft.TaskRef != p.TaskRef {
		return reject(RejectRule, "草案计划属于任务 %s，与改派任务 %s 不符", draft.TaskRef, p.TaskRef)
	}
	if p.RouteRef == "" {
		return reject(RejectRule, "改派必须声明被钉死的航线")
	}
	for _, r := range e.state.Retasks {
		if r.Status == RetaskProposed && r.TaskRef == p.TaskRef {
			return reject(RejectRule, "任务 %s 已有待确认改派 %s，不得叠加新改派", p.TaskRef, r.RetaskID)
		}
	}
	return nil
}

// humanOverride 判断事件是否附带人工 FORCE 指令（作用于事件自身时刻），
// 用于航线钉死等场景：人工可以介入，自动策略不可以。
func (e *Engine) humanOverride(env domain.Envelope, verb, arg string, at time.Time) bool {
	if env.Operator == "" || env.Operator == domain.RoleSystem {
		return false
	}
	d := e.state.ForceDecisionAt(verb, at)
	if d == nil {
		return false
	}
	return decisionArg(d.Decision, verb) == arg
}

// automate 在事件生效后执行确定性的连锁裁决。派生事件同样入日志、进状态，
// 因此断链恢复与事后还原能完整复现每一次自动决定及其责任归属。
func (e *Engine) automate(parent domain.Envelope) []DerivedEffect {
	var effects []DerivedEffect
	switch p := mustPayload(parent).(type) {
	case *domain.CallRequestedPayload:
		effects = append(effects, e.scheduleAdmission(*p, parent)...)
	case *domain.CallEndedPayload:
		effects = append(effects, e.drainQueues(parent)...)
	case *domain.StationCoveragePayload:
		effects = append(effects, e.onCoverageChanged(*p, parent)...)
	case *domain.PersonSightedPayload:
		effects = append(effects, e.proposeObservedRetask(domain.TriggerPersonFound, p.TaskRef,
			fmt.Sprintf("失联人员位置更新（观测 %s，位置 %s，队伍 %s），建议改派前往核实救援",
				p.SightingID, p.LocationRef, p.TeamRef),
			parent.PayloadDigest, parent, "PERSON", p.SightingID)...)
	case *domain.SecondaryDisasterPayload:
		summary := fmt.Sprintf("出现次生灾害 %s（观测 %s），需要规避并调整任务", p.Kind, p.ObservationID)
		if p.NewAreaVersion > 0 {
			summary += fmt.Sprintf("，区域已升至版本 %d", p.NewAreaVersion)
		}
		effects = append(effects, e.proposeObservedRetask(domain.TriggerSecondary, p.TaskRef,
			summary, parent.PayloadDigest, parent, "SECONDARY", p.ObservationID)...)
	case *domain.AirspaceNoticePayload:
		effects = append(effects, e.proposeObservedRetask(domain.TriggerAirspace, p.TaskRef,
			fmt.Sprintf("空域冲突通告 %s（空域 %s，类型 %s），需要立即避让改派",
				p.NoticeID, p.AirspaceRef, p.Kind),
			parent.PayloadDigest, parent, "AIRSPACE", p.NoticeID)...)
	case *domain.RetaskProposedPayload:
		// 任何受控改派一提案即钉死航线，确认/拒绝前自动策略不得改航线。
		// emitDerived 自身幂等，重复提案不会产生重复钉事件。
		pin := domain.RoutePinnedPayload{RouteRef: p.RouteRef, RetaskID: p.RetaskID, PinnedBy: domain.RoleSystem}
		if _, ok := e.emitDerived(domain.TypeRoutePinned, parent, p.RouteRef, pin); ok {
			effects = append(effects, DerivedEffect{Type: domain.TypeRoutePinned, Summary: "改派 " + p.RetaskID + " 已钉死航线 " + p.RouteRef})
		}
	case *domain.RetaskConfirmedPayload:
		effects = append(effects, e.activateRetaskPlan(*p, parent)...)
	case *domain.ManualDirectivePayload:
		switch p.Kind {
		case domain.ManualForce:
			effects = append(effects, e.applyForceDecisions(*p, parent)...)
		case domain.ManualResumeCapacity:
			// 解除冻结后立即尝试按序回放被压住的队列（补偿优先）。
			effects = append(effects, e.drainQueues(parent)...)
		}
	}
	return effects
}

// scheduleAdmission 裁决一通新通话：有空位直接接入；高优先级在未冻结时可抢占
// 普通通话并留下补偿条目；其余情况排队。
func (e *Engine) scheduleAdmission(req domain.CallRequestedPayload, parent domain.Envelope) []DerivedEffect {
	at, _ := domain.ParseTime(parent.OccurredAt)
	st := e.state.Stations[req.StationRef]

	// 人工 FORCE 要求接入该通话：立即强制接入，不受容量与冻结限制。
	if d := e.forceForCall("ADMIT_CALL", req.CallID, at); d != nil {
		return e.emitAdmit(req.CallID, req.StationRef, "", true, parent, "人工强制接入")
	}

	if e.state.FreeCapacity(req.StationRef) > 0 {
		return e.emitAdmit(req.CallID, req.StationRef, "", false, parent, "有空余容量，直接接入")
	}

	highPriority := req.Priority == 1
	frozen := e.state.FrozenStations(req.StationRef, at)

	if highPriority && !frozen {
		if victim := e.pickVictim(req.StationRef, at); victim != "" {
			return e.emitPreemption(req, victim, parent)
		}
		// 没有可抢占对象（全部为高优先级或受保护）：高优先级也只能排队，原因留痕。
		return e.emitQueued(req.CallID, req.StationRef, QueueNoVictim, parent)
	}
	if highPriority && frozen {
		// 冻结期：自动策略禁止抢占，人工指令优先于救援优先级。
		return e.emitQueued(req.CallID, req.StationRef, QueueFrozen, parent)
	}
	_ = st
	return e.emitQueued(req.CallID, req.StationRef, QueueCongested, parent)
}

// pickVictim 选择被抢占者：在网的普通通话中优先级最低者；
// 被人工 FORCE 保护的通话绝不能被选中。
func (e *Engine) pickVictim(stationRef string, at time.Time) string {
	victim := ""
	worst := 0
	for _, c := range e.state.Calls {
		if c.StationRef != stationRef || c.Status != CallActive {
			continue
		}
		if c.Priority == 1 || c.Protected || e.state.ProtectedCall(c.CallID, at) {
			continue
		}
		if victim == "" || c.Priority > worst {
			victim = c.CallID
			worst = c.Priority
		}
	}
	return victim
}

// emitPreemption 记录"抢占裁决 + 补偿条目"，随后接入高优先级通话。
func (e *Engine) emitPreemption(req domain.CallRequestedPayload, victimCallID string, parent domain.Envelope) []DerivedEffect {
	var effects []DerivedEffect
	compID := "COMP-" + req.CallID + "-" + victimCallID
	preempt := domain.CallPreemptedPayload{
		StationRef:      req.StationRef,
		PreemptedCallID: victimCallID,
		PriorityCallID:  req.CallID,
		CompensationID:  compID,
		DecidedBy:       domain.RoleSystem,
	}
	if env, ok := e.emitDerived(domain.TypeCallPreempted, parent, req.StationRef, preempt); ok {
		effects = append(effects, derivedSummary(env, "高优先级通话 %s 抢占 %s，补偿条目 %s",
			req.CallID, victimCallID, compID))
	}
	effects = append(effects, e.emitAdmit(req.CallID, req.StationRef, "", false, parent,
		"高优先级通话抢占后接入")...)
	return effects
}

// emitAdmit 接入一通通话。
func (e *Engine) emitAdmit(callID, stationRef, viaComp string, forced bool, parent domain.Envelope, summary string) []DerivedEffect {
	admit := domain.CallAdmittedPayload{
		CallID: callID, StationRef: stationRef,
		ViaCompensationID: viaComp, Forced: forced,
	}
	if env, ok := e.emitDerived(domain.TypeCallAdmitted, parent, stationRef, admit); ok {
		return []DerivedEffect{derivedSummary(env, "%s", summary)}
	}
	return nil
}

// emitQueued 让通话进入等待队列并留下排队原因。
func (e *Engine) emitQueued(callID, stationRef, reason string, parent domain.Envelope) []DerivedEffect {
	queued := domain.CallQueuedPayload{CallID: callID, StationRef: stationRef, Reason: reason}
	if env, ok := e.emitDerived(domain.TypeCallQueued, parent, stationRef, queued); ok {
		return []DerivedEffect{derivedSummary(env, "通话 %s 进入等待队列（%s）", callID, reason)}
	}
	return nil
}

// drainQueues 在容量可能恢复时处理所有基站的队列，基站按引用排序保证顺序确定。
func (e *Engine) drainQueues(parent domain.Envelope) []DerivedEffect {
	var effects []DerivedEffect
	for _, stationRef := range e.stationRefsSorted() {
		effects = append(effects, e.drainStation(stationRef, parent)...)
	}
	return effects
}

// drainStation 处理单个基站：补偿队列只在有空位时优先回放；普通等待队列队首
// 重新执行完整裁决（可抢占），仍只能排队则停止，保持 FIFO。
func (e *Engine) drainStation(stationRef string, parent domain.Envelope) []DerivedEffect {
	var effects []DerivedEffect
	for e.state.FreeCapacity(stationRef) > 0 {
		entry, ok := e.peekCompensation(stationRef)
		if !ok {
			break
		}
		effects = append(effects, e.replayOne(entry, parent)...)
	}
	for {
		entry, ok := e.peekWaiting(stationRef)
		if !ok {
			break
		}
		c := e.state.Calls[entry.CallID]
		if c == nil {
			break
		}
		at, _ := domain.ParseTime(parent.OccurredAt)
		if !e.canLeaveWaiting(c, at) {
			break
		}
		effects = append(effects, e.scheduleAdmission(domain.CallRequestedPayload{
			CallID: c.CallID, StationRef: c.StationRef, TeamRef: c.TeamRef, Priority: c.Priority,
		}, parent)...)
	}
	return effects
}

// stationRefsSorted 返回当前已知基站引用的有序列表。
func (e *Engine) stationRefsSorted() []string {
	set := map[string]bool{}
	for _, c := range e.state.Calls {
		set[c.StationRef] = true
	}
	for ref := range e.state.Stations {
		set[ref] = true
	}
	out := make([]string, 0, len(set))
	for ref := range set {
		out = append(out, ref)
	}
	sort.Strings(out)
	return out
}

// canLeaveWaiting 判断排队通话在当前时刻能否出队：有空位，或者高优先级且
// 未冻结、且存在可抢占的普通通话。
func (e *Engine) canLeaveWaiting(c *CallView, at time.Time) bool {
	if e.state.FreeCapacity(c.StationRef) > 0 {
		return true
	}
	if c.Priority != 1 || e.state.FrozenStations(c.StationRef, at) {
		return false
	}
	return e.pickVictim(c.StationRef, at) != ""
}

// peekCompensation 取补偿队列队首（副本），队列本身由回放事件投影修改。
func (e *Engine) peekCompensation(stationRef string) (QueueEntry, bool) {
	for _, entry := range e.state.CompensationQueue(stationRef) {
		return entry, true
	}
	return QueueEntry{}, false
}

func (e *Engine) peekWaiting(stationRef string) (QueueEntry, bool) {
	for _, entry := range e.state.WaitingQueue(stationRef) {
		return entry, true
	}
	return QueueEntry{}, false
}

// replayOne 回放一条补偿：先 compensation.replayed 再 call.admitted。
func (e *Engine) replayOne(entry QueueEntry, parent domain.Envelope) []DerivedEffect {
	var effects []DerivedEffect
	replay := domain.CompensationReplayedPayload{
		CompensationID: entry.ID, CallID: entry.CallID, StationRef: entry.StationRef,
	}
	if env, ok := e.emitDerived(domain.TypeCompensationReplayed, parent, entry.StationRef, replay); ok {
		effects = append(effects, derivedSummary(env, "补偿回放：通话 %s（条目 %s）重新接通",
			entry.CallID, entry.ID))
	}
	admit := domain.CallAdmittedPayload{
		CallID: entry.CallID, StationRef: entry.StationRef,
		ViaCompensationID: entry.ID,
	}
	if env, ok := e.emitDerived(domain.TypeCallAdmitted, parent, entry.StationRef, admit); ok {
		effects = append(effects, derivedSummary(env, "补偿通话 %s 接入", entry.CallID))
	}
	return effects
}

// admitWaiting 从普通等待队列接入一通通话。
func (e *Engine) admitWaiting(entry QueueEntry, parent domain.Envelope) []DerivedEffect {
	queued := domain.CallQueuedPayload{}
	_ = queued
	return e.emitAdmit(entry.CallID, entry.StationRef, "", false, parent, "排队通话按序接入")
}

// onCoverageChanged 根据覆盖变化执行自动化：
//   - 覆盖下降（基站失活、容量低于在网通话数，或容量较上一份上报绝对下降）时
//     自动生成受控改派提案，提案即钉死航线，仍须 NETWORK_OPS 岗位确认才生效；
//   - 覆盖恢复（重新激活、容量回升）时尝试回放补偿与等待队列。
func (e *Engine) onCoverageChanged(cov domain.StationCoveragePayload, parent domain.Envelope) []DerivedEffect {
	current := e.state.Stations[cov.StationRef]
	prevCapacity := 0
	if current != nil {
		prevCapacity = current.prevCapacity
	}
	degraded := !cov.Active ||
		cov.Capacity < e.state.ActiveCalls(cov.StationRef) ||
		cov.Capacity < prevCapacity
	if degraded {
		return e.proposeCoverageRetask(cov, parent)
	}
	// 恢复且有队列待处理：统一走补偿优先的单基站排空。
	return e.drainStation(cov.StationRef, parent)
}

// proposeCoverageRetask 生成覆盖下降改派提案并钉死航线。
func (e *Engine) proposeCoverageRetask(cov domain.StationCoveragePayload, parent domain.Envelope) []DerivedEffect {
	taskRef, plan, routeRef := e.locateTaskByStation(cov.StationRef)
	if taskRef == "" {
		return nil
	}
	// 同一任务已有待确认改派时不叠加（空域冲突等提案同样会阻断）。
	for _, r := range e.state.Retasks {
		if r.Status == RetaskProposed && r.TaskRef == taskRef {
			return nil
		}
	}
	retaskID := derivedID(parent, "COVERAGE_DROP", cov.StationRef)
	proposal := domain.RetaskProposedPayload{
		RetaskID:       retaskID,
		TaskRef:        taskRef,
		TriggerType:    domain.TriggerCoverageDrop,
		RequiredRole:   domain.RequiredRoleForTrigger(domain.TriggerCoverageDrop),
		Summary:        fmt.Sprintf("基站 %s 覆盖下降（capacity=%d active=%t），建议改派恢复覆盖", cov.StationRef, cov.Capacity, cov.Active),
		CurrentPlanID:  plan.PlanID,
		ProposedPlanID: plan.PlanID, // 恢复方案草案由指挥/网络岗补签后在确认时指定
		RouteRef:       routeRef,
		EvidenceDigest: parent.PayloadDigest,
	}
	var effects []DerivedEffect
	if env, ok := e.emitDerived(domain.TypeRetaskProposed, parent, taskRef, proposal); ok {
		effects = append(effects, derivedSummary(env, "覆盖下降自动生成改派 %s，航线 %s 已钉死，等待 %s 确认",
			retaskID, routeRef, proposal.RequiredRole))
		// 提案即钉死航线，阻断一切自动航线策略。
		pin := domain.RoutePinnedPayload{RouteRef: routeRef, RetaskID: retaskID, PinnedBy: domain.RoleSystem}
		if pinEnv, ok := e.emitDerived(domain.TypeRoutePinned, parent, routeRef, pin); ok {
			effects = append(effects, derivedSummary(pinEnv, "改派 %s 已钉死航线 %s", retaskID, routeRef))
		}
	}
	return effects
}

// proposeObservedRetask 为人员位置、次生灾害、空域冲突三类观测生成受控改派提案，
// 并立即钉死航线。提案必须由触发类型对应的岗位确认；恢复/改派方案计划由该岗位
// 在确认时通过 plan_id 指定（草案未补签前先以当前计划占位）。
func (e *Engine) proposeObservedRetask(trigger, taskRef, summary, evidenceDigest string,
	parent domain.Envelope, idParts ...string) []DerivedEffect {
	plan := e.state.ActivePlan(taskRef)
	if plan == nil {
		return nil
	}
	for _, r := range e.state.Retasks {
		if r.Status == RetaskProposed && r.TaskRef == taskRef {
			return nil
		}
	}
	retaskID := derivedID(parent, append([]string{trigger}, idParts...)...)
	proposal := domain.RetaskProposedPayload{
		RetaskID:       retaskID,
		TaskRef:        taskRef,
		TriggerType:    trigger,
		RequiredRole:   domain.RequiredRoleForTrigger(trigger),
		Summary:        summary,
		CurrentPlanID:  plan.PlanID,
		ProposedPlanID: plan.PlanID,
		RouteRef:       plan.RouteRef,
		EvidenceDigest: evidenceDigest,
	}
	var effects []DerivedEffect
	if env, ok := e.emitDerived(domain.TypeRetaskProposed, parent, taskRef, proposal); ok {
		effects = append(effects, derivedSummary(env, "%s 自动生成改派 %s，航线 %s 已钉死，等待 %s 确认",
			trigger, retaskID, plan.RouteRef, proposal.RequiredRole))
		pin := domain.RoutePinnedPayload{RouteRef: plan.RouteRef, RetaskID: retaskID, PinnedBy: domain.RoleSystem}
		if pinEnv, ok := e.emitDerived(domain.TypeRoutePinned, parent, plan.RouteRef, pin); ok {
			effects = append(effects, derivedSummary(pinEnv, "改派 %s 已钉死航线 %s", retaskID, plan.RouteRef))
		}
	}
	return effects
}

// activateRetaskPlan 在岗位确认后激活确认时选定的计划，完成受控改派。
func (e *Engine) activateRetaskPlan(confirm domain.RetaskConfirmedPayload, parent domain.Envelope) []DerivedEffect {
	r, ok := e.state.Retasks[confirm.RetaskID]
	if !ok {
		return nil
	}
	chosen := confirm.PlanID
	if chosen == "" {
		chosen = r.ProposedPlanID
	}
	activation := domain.PlanActivatedPayload{
		PlanID: chosen, TaskRef: r.TaskRef,
		ActivatedBy: confirm.ConfirmedBy,
	}
	if env, ok := e.emitDerived(domain.TypePlanActivated, parent, r.TaskRef, activation); ok {
		return []DerivedEffect{derivedSummary(env, "改派 %s 经 %s 确认，计划 %s 激活",
			r.RetaskID, confirm.ConfirmedBy, chosen)}
	}
	return nil
}

// applyForceDecisions 让人工 FORCE 指令即时生效。自动策略只读取、从不产生这些决定。
func (e *Engine) applyForceDecisions(manual domain.ManualDirectivePayload, parent domain.Envelope) []DerivedEffect {
	var effects []DerivedEffect
	verbs := parseDecisionVerbs(manual.Decision)
	if callID := verbs["ADMIT_CALL"]; callID != "" {
		c, ok := e.state.Calls[callID]
		if ok && c.Status != CallActive && c.Status != CallEnded {
			effects = append(effects, e.emitAdmit(callID, c.StationRef, "", true, parent,
				"人工 FORCE 强制接入")...)
		}
	}
	return effects
}

func (e *Engine) forceForCall(verb, callID string, at time.Time) *DirectiveView {
	d := e.state.ForceDecisionAt(verb, at)
	if d == nil || decisionArg(d.Decision, verb) != callID {
		return nil
	}
	return d
}

// locateTaskByStation 通过激活计划找到基站当前服务的任务与航线。
func (e *Engine) locateTaskByStation(stationRef string) (taskRef string, plan *PlanView, routeRef string) {
	for _, t := range e.state.Tasks {
		if p := e.state.ActivePlan(t.Ref); p != nil && p.StationRef == stationRef {
			return t.Ref, p, p.RouteRef
		}
	}
	return "", nil, ""
}

// emitDerived 生成并落盘一条确定性派生事件，返回是否实际写入（幂等失败返回 false）。
func (e *Engine) emitDerived(eventType string, parent domain.Envelope, subject string, payload any) (domain.Envelope, bool) {
	id := derivedID(parent, eventType, subject, stablePayloadKey(payload))
	if e.seen[id] || e.derivedSeen[id] {
		return domain.Envelope{}, false
	}
	raw, digest, err := domain.CanonicalDigest(payload)
	if err != nil {
		return domain.Envelope{}, false
	}
	env := domain.Envelope{
		SchemaVersion:  domain.SchemaVersion,
		EventID:        id,
		SubjectRef:     subject,
		Type:           eventType,
		OccurredAt:     parent.OccurredAt, // 派生裁决与触发事实同一业务时刻
		Source:         "controller:" + parent.Source,
		SourceSequence: parent.SourceSequence,
		DeviceSerial:   parent.DeviceSerial,
		Operator:       domain.RoleSystem,
		Payload:        raw,
		PayloadDigest:  digest,
	}
	offset, err := e.log.Append(env)
	if err != nil {
		return domain.Envelope{}, false
	}
	_ = offset
	e.seen[id] = true
	e.derivedSeen[id] = true
	e.state.Apply(env)
	return env, true
}

// derivedID 由父事件与裁决要素确定性地生成事件标识。
func derivedID(parent domain.Envelope, parts ...string) string {
	joined := parent.EventID + "|" + strings.Join(parts, "|")
	sum := sha256.Sum256([]byte(joined))
	return "DERIVED-" + hex.EncodeToString(sum[:])[:20]
}

// stablePayloadKey 给载荷一个稳定字符串参与去重（JSON 规范化后取值）。
func stablePayloadKey(payload any) string {
	raw, _, err := domain.CanonicalDigest(payload)
	if err != nil {
		return ""
	}
	return string(raw)
}

func derivedSummary(env domain.Envelope, format string, args ...any) DerivedEffect {
	return DerivedEffect{
		EventID: env.EventID,
		Type:    env.Type,
		Summary: fmt.Sprintf(format, args...),
	}
}

// PendingGap 返回某来源当前的序号缺口（用于运维观察离线补传进度）。
func (e *Engine) PendingGap(source string) (next int64, buffered []int64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	next = e.nextSeq[source]
	for seq := range e.pending[source] {
		buffered = append(buffered, seq)
	}
	return
}
