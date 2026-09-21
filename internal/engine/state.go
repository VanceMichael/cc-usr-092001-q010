package engine

import (
	"sort"
	"time"

	"example.com/batch-092001-q010/internal/domain"
)

// 通话与队列状态。
const (
	CallRequested = "REQUESTED" // 已请求，尚未裁决
	CallWaiting   = "WAITING"   // 普通排队（拥塞/冻结/无可抢占对象）
	CallPreempted = "PREEMPTED" // 被高优先级通话抢占，在补偿队列中
	CallActive    = "ACTIVE"
	CallEnded     = "ENDED"
)

// 改派状态。
const (
	RetaskProposed  = "PROPOSED"
	RetaskConfirmed = "CONFIRMED"
	RetaskRejected  = "REJECTED"
)

// 排队原因。
const (
	QueueCongested = "CONGESTED" // 容量满，普通排队
	QueueFrozen    = "FROZEN"    // 人工冻结自动抢占
	QueueNoVictim  = "NO_VICTIM" // 高优先级但没有可抢占对象
)

// PhaseRecord 保存一次阶段变化的完整事实。
type PhaseRecord struct {
	TaskRef     string          `json:"task_ref"`
	FromPhase   string          `json:"from_phase"`
	ToPhase     string          `json:"to_phase"`
	DroneSerial string          `json:"drone_serial"`
	Reason      string          `json:"reason"`
	PlanID      string          `json:"plan_id,omitempty"`
	OccurredAt  time.Time       `json:"-"`
	Envelope    domain.Envelope `json:"-"`
}

// CallView 是通话的投影状态。
type CallView struct {
	CallID      string `json:"call_id"`
	StationRef  string `json:"station_ref"`
	TeamRef     string `json:"team_ref,omitempty"`
	Priority    int    `json:"priority"`
	Status      string `json:"status"`
	Protected   bool   `json:"protected"`    // 人工 FORCE 保护，自动策略不可抢占
	ForcedAdmit bool   `json:"forced_admit"` // 经人工强制指令接入
	Subject     string `json:"subject,omitempty"`
}

// QueueEntry 是补偿队列或普通等待队列中的一条。
type QueueEntry struct {
	ID         string    `json:"id"` // 补偿条目号或通话号
	CallID     string    `json:"call_id"`
	StationRef string    `json:"station_ref"`
	EnqueuedAt time.Time `json:"-"`
	Reason     string    `json:"reason,omitempty"`
}

// RetaskView 是受控改派的投影状态。
type RetaskView struct {
	domain.RetaskProposedPayload
	Status       string     `json:"status"`
	ProposedAt   time.Time  `json:"-"`
	ConfirmedAt  *time.Time `json:"-"`
	ConfirmedBy  string     `json:"confirmed_by,omitempty"`
	RejectedBy   string     `json:"rejected_by,omitempty"`
	RejectReason string     `json:"reject_reason,omitempty"`
}

// PlanView 保存计划及其签署/激活事实。
type PlanView struct {
	domain.PlanSignedPayload
	SignedAt    time.Time       `json:"-"`
	SignedByEnv domain.Envelope `json:"-"`
	Activated   bool            `json:"activated"`
	ActivatedAt *time.Time      `json:"-"`
	ActivatedBy string          `json:"activated_by,omitempty"`
}

// DirectiveView 保存人工指令。
type DirectiveView struct {
	domain.ManualDirectivePayload
	OccurredAt time.Time `json:"-"`
}

// ReconView 保存侦察成果。
type ReconView struct {
	domain.ReconResultPayload
	OccurredAt time.Time `json:"-"`
}

// State 是事件日志在某一时刻的完整投影。零值即可用，事件通过 Apply 进入。
// 所有时间判断都使用事件自身的 occurred_at，而不是处理时刻。
type State struct {
	Areas      map[string]*areaState
	Drones     map[string]*droneState
	Clearances map[string]*clearanceState
	Plans      map[string]*PlanView
	Tasks      map[string]*taskState
	Routes     map[string]*routeState
	Stations   map[string]*stationState
	Demands    map[string]domain.GroundTeamDemandPayload
	Priorities map[string]int
	Retasks    map[string]*RetaskView
	Calls      map[string]*CallView
	Recons     map[string]*ReconView
	Directives []DirectiveView

	// 补偿队列：被抢占的普通通话，容量恢复后优先回放。
	compensation map[string][]*QueueEntry
	// 普通等待队列：容量满/冻结/无可抢占对象时排队。
	waiting map[string][]*QueueEntry
	// 显式航线钉死事件（retask 推断的钉死不在这里，见 RoutePinned）。
	manualPins map[string]string
}

type areaState struct {
	Latest  domain.AreaVersionedPayload
	History []domain.AreaVersionedPayload
}

type droneState struct {
	Registration domain.DroneRegisteredPayload
	Telemetry    *domain.DroneTelemetryPayload
	TelemetryAt  time.Time
	Fuel         *domain.DroneFuelBatteryPayload
	FuelAt       time.Time
}

type clearanceState struct {
	domain.ClearanceGrantedPayload
	GrantedAt time.Time
}

type taskState struct {
	Ref          string
	Phase        string
	PhaseHistory []PhaseRecord
	ActivePlanID string
}

type routeState struct {
	Ref       string
	Waypoints []domain.Waypoint
	UpdatedBy string
}

type stationState struct {
	domain.StationCoveragePayload
	UpdatedAt time.Time
	// 控制器依据通话事件自行核算的在网通话数；coverage 上报的 used 仅作现场参考。
	activeCalls int
	// prevCapacity 保存上一份覆盖上报的容量，用于识别"覆盖下降"。
	prevCapacity int
}

// NewState 返回空投影。
func NewState() *State {
	return &State{
		Areas:        map[string]*areaState{},
		Drones:       map[string]*droneState{},
		Clearances:   map[string]*clearanceState{},
		Plans:        map[string]*PlanView{},
		Tasks:        map[string]*taskState{},
		Routes:       map[string]*routeState{},
		Stations:     map[string]*stationState{},
		Demands:      map[string]domain.GroundTeamDemandPayload{},
		Priorities:   map[string]int{},
		Retasks:      map[string]*RetaskView{},
		Calls:        map[string]*CallView{},
		Recons:       map[string]*ReconView{},
		compensation: map[string][]*QueueEntry{},
		waiting:      map[string][]*QueueEntry{},
		manualPins:   map[string]string{},
	}
}

// TaskPhase 返回任务当前阶段；从未出现阶段事件的任务视为待命。
func (s *State) TaskPhase(ref string) string {
	if t, ok := s.Tasks[ref]; ok {
		return t.Phase
	}
	return domain.PhaseStandby
}

// ActivePlan 返回任务当前激活的计划视图。
func (s *State) ActivePlan(taskRef string) *PlanView {
	if t, ok := s.Tasks[taskRef]; ok && t.ActivePlanID != "" {
		return s.Plans[t.ActivePlanID]
	}
	return nil
}

// ActiveCalls 返回基站在网通话数（控制器核算口径）。
func (s *State) ActiveCalls(stationRef string) int {
	if st, ok := s.Stations[stationRef]; ok {
		return st.activeCalls
	}
	return 0
}

// FreeCapacity 返回基站剩余可承载通话数。
func (s *State) FreeCapacity(stationRef string) int {
	st, ok := s.Stations[stationRef]
	if !ok || !st.Active || st.Capacity <= 0 {
		return 0
	}
	free := st.Capacity - st.activeCalls
	if free < 0 {
		return 0
	}
	return free
}

// CompensationQueue 返回某基站补偿队列的副本。
func (s *State) CompensationQueue(stationRef string) []QueueEntry {
	return copyEntries(s.compensation[stationRef])
}

// WaitingQueue 返回某基站普通等待队列的副本。
func (s *State) WaitingQueue(stationRef string) []QueueEntry {
	return copyEntries(s.waiting[stationRef])
}

func copyEntries(in []*QueueEntry) []QueueEntry {
	out := make([]QueueEntry, 0, len(in))
	for _, e := range in {
		out = append(out, *e)
	}
	return out
}

// RoutePinned 判断航线是否处于钉死状态：存在指向该航线且尚未处理的改派，
// 或有显式 route.pinned 事件尚未随改派处理而解除。
func (s *State) RoutePinned(routeRef string) (bool, string) {
	if retaskID, ok := s.manualPins[routeRef]; ok {
		return true, retaskID
	}
	for _, r := range s.Retasks {
		if r.Status == RetaskProposed && r.RouteRef == routeRef {
			return true, r.RetaskID
		}
	}
	return false, ""
}

// FrozenStations 判断自动抢占是否被人工冻结。支持 "*" 全局与 "STATION:<ref>" 两级作用域。
func (s *State) FrozenStations(stationRef string, at time.Time) bool {
	frozenGlobal := false
	frozenStation := false
	for _, d := range s.Directives {
		if d.OccurredAt.After(at) {
			continue
		}
		switch d.Kind {
		case domain.ManualFreezeCapacity:
			switch d.Scope {
			case "*":
				frozenGlobal = true
			case "STATION:" + stationRef:
				frozenStation = true
			}
		case domain.ManualResumeCapacity:
			switch d.Scope {
			case "*":
				frozenGlobal = false
			case "STATION:" + stationRef:
				frozenStation = false
			}
		}
	}
	return frozenGlobal || frozenStation
}

// ForceDecisionAt 返回不晚于 at 时刻、针对某动词的最新 FORCE 指令（若存在）。
// 自动策略永远不会修改或撤销人工指令，这里只做只读查询。
func (s *State) ForceDecisionAt(verb string, at time.Time) *DirectiveView {
	var found *DirectiveView
	for i := range s.Directives {
		d := &s.Directives[i]
		if d.Kind != domain.ManualForce || d.OccurredAt.After(at) {
			continue
		}
		if !hasDecisionVerb(d.Decision, verb) {
			continue
		}
		if found == nil || d.OccurredAt.After(found.OccurredAt) {
			found = d
		}
	}
	return found
}

// ProtectedCall 判断通话是否被人工 FORCE 保护，自动抢占必须跳过它。
func (s *State) ProtectedCall(callID string, at time.Time) bool {
	for _, d := range s.Directives {
		if d.Kind == domain.ManualForce && !d.OccurredAt.After(at) &&
			hasDecisionVerb(d.Decision, "PROTECT_CALL") &&
			decisionArg(d.Decision, "PROTECT_CALL") == callID {
			return true
		}
	}
	return false
}

// ClearanceValidAt 判断无人机在 t 时刻是否持有有效起飞/空域许可。
func (s *State) ClearanceValidAt(droneSerial string, at time.Time) bool {
	for _, c := range s.Clearances {
		if c.DroneSerial != droneSerial {
			continue
		}
		validUntil, err := domain.ParseTime(c.ValidUntil)
		if err != nil {
			continue
		}
		if !at.Before(c.GrantedAt) && !at.After(validUntil) {
			return true
		}
	}
	return false
}

// OverrideClearance 判断是否有人工 FORCE 指令为该任务豁免许可要求。
func (s *State) OverrideClearance(taskRef string, at time.Time) bool {
	for _, d := range s.Directives {
		if d.Kind == domain.ManualForce && !d.OccurredAt.After(at) &&
			hasDecisionVerb(d.Decision, "OVERRIDE_CLEARANCE") &&
			(decisionArg(d.Decision, "OVERRIDE_CLEARANCE") == taskRef || d.Scope == "TASK:"+taskRef) {
			return true
		}
	}
	return false
}

// Apply 把一条事件投影进状态。它假定事件已经过摄入校验（重放与引擎派生事件
// 都走这里），因此只做幂等的状态演进，不做业务拒绝——历史事实不可被重写。
func (s *State) Apply(env domain.Envelope) {
	at, _ := domain.ParseTime(env.OccurredAt)
	payload, err := domain.DecodePayload(env.Type, env.Payload)
	if err != nil {
		return
	}
	switch p := payload.(type) {
	case *domain.AreaVersionedPayload:
		a, ok := s.Areas[p.AreaID]
		if !ok {
			a = &areaState{}
			s.Areas[p.AreaID] = a
		}
		a.Latest = *p
		a.History = append(a.History, *p)

	case *domain.DroneRegisteredPayload:
		s.Drones[p.DroneSerial] = &droneState{Registration: *p}

	case *domain.DroneTelemetryPayload:
		// 遥测只更新位置，绝不触碰任务阶段。
		if d, ok := s.Drones[p.DroneSerial]; ok {
			d.Telemetry = p
			d.TelemetryAt = at
		}

	case *domain.DroneFuelBatteryPayload:
		if d, ok := s.Drones[p.DroneSerial]; ok {
			d.Fuel = p
			d.FuelAt = at
		}

	case *domain.ClearanceGrantedPayload:
		s.Clearances[p.ClearanceID] = &clearanceState{ClearanceGrantedPayload: *p, GrantedAt: at}

	case *domain.PlanSignedPayload:
		s.Plans[p.PlanID] = &PlanView{PlanSignedPayload: *p, SignedAt: at, SignedByEnv: env}

	case *domain.PlanActivatedPayload:
		if plan, ok := s.Plans[p.PlanID]; ok {
			now := at
			plan.Activated = true
			plan.ActivatedAt = &now
			plan.ActivatedBy = p.ActivatedBy
			t := s.ensureTask(p.TaskRef)
			t.ActivePlanID = p.PlanID
		}

	case *domain.TaskPhaseChangedPayload:
		t := s.ensureTask(p.TaskRef)
		t.Phase = p.ToPhase
		t.PhaseHistory = append(t.PhaseHistory, PhaseRecord{
			TaskRef: p.TaskRef, FromPhase: p.FromPhase, ToPhase: p.ToPhase,
			DroneSerial: p.DroneSerial, Reason: p.Reason, PlanID: p.PlanID,
			OccurredAt: at, Envelope: env,
		})
		if p.PlanID != "" {
			t.ActivePlanID = p.PlanID
		}

	case *domain.RouteUpdatedPayload:
		s.Routes[p.RouteRef] = &routeState{Ref: p.RouteRef, Waypoints: p.Waypoints, UpdatedBy: p.ChangedBy}

	case *domain.RoutePinnedPayload:
		s.manualPins[p.RouteRef] = p.RetaskID

	case *domain.StationCoveragePayload:
		keptCalls := 0
		prevCapacity := 0
		if old, ok := s.Stations[p.StationRef]; ok {
			keptCalls = old.activeCalls
			prevCapacity = old.Capacity
		}
		s.Stations[p.StationRef] = &stationState{
			StationCoveragePayload: *p,
			UpdatedAt:              at,
			activeCalls:            keptCalls,
			prevCapacity:           prevCapacity,
		}

	case *domain.GroundTeamDemandPayload:
		s.Demands[p.DemandID] = *p

	case *domain.PrioritySetPayload:
		s.Priorities[p.SubjectRef] = p.Priority

	case *domain.RetaskProposedPayload:
		s.Retasks[p.RetaskID] = &RetaskView{RetaskProposedPayload: *p, Status: RetaskProposed, ProposedAt: at}

	case *domain.RetaskConfirmedPayload:
		if r, ok := s.Retasks[p.RetaskID]; ok {
			now := at
			r.Status = RetaskConfirmed
			r.ConfirmedAt = &now
			r.ConfirmedBy = p.ConfirmedBy
			delete(s.manualPins, r.RouteRef)
		}

	case *domain.RetaskRejectedPayload:
		if r, ok := s.Retasks[p.RetaskID]; ok {
			r.Status = RetaskRejected
			r.RejectedBy = p.RejectedBy
			r.RejectReason = p.Reason
			delete(s.manualPins, r.RouteRef)
		}

	case *domain.CallRequestedPayload:
		if _, exists := s.Calls[p.CallID]; !exists {
			s.Calls[p.CallID] = &CallView{
				CallID: p.CallID, StationRef: p.StationRef, TeamRef: p.TeamRef,
				Priority: p.Priority, Status: CallRequested, Subject: p.Subject,
			}
		}

	case *domain.CallQueuedPayload:
		if c, ok := s.Calls[p.CallID]; ok {
			c.Status = CallWaiting
			s.waiting[p.StationRef] = appendQueue(s.waiting[p.StationRef], &QueueEntry{
				ID: p.CallID, CallID: p.CallID, StationRef: p.StationRef,
				EnqueuedAt: at, Reason: p.Reason,
			})
		}

	case *domain.CallPreemptedPayload:
		// 被抢占者离开容量、进入补偿队列；补偿条目必须留存。
		if victim, ok := s.Calls[p.PreemptedCallID]; ok {
			victim.Status = CallPreempted
			if st, ok := s.Stations[p.StationRef]; ok {
				st.activeCalls--
				if st.activeCalls < 0 {
					st.activeCalls = 0
				}
			}
			s.compensation[p.StationRef] = appendQueue(s.compensation[p.StationRef], &QueueEntry{
				ID: p.CompensationID, CallID: p.PreemptedCallID, StationRef: p.StationRef,
				EnqueuedAt: at,
			})
		}
		// 获胜的高优先级通话由紧随其后的 call.admitted 事件记录接入，
		// 抢占事件本身只负责"让位 + 补偿留痕"，避免重复计数。

	case *domain.CallAdmittedPayload:
		if c, ok := s.Calls[p.CallID]; ok {
			c.Status = CallActive
			if p.Forced {
				c.ForcedAdmit = true
			}
			s.waiting[p.StationRef] = dropEntry(s.waiting[p.StationRef], p.CallID)
			if st, ok := s.Stations[p.StationRef]; ok {
				st.activeCalls++
			}
		}

	case *domain.CallEndedPayload:
		if c, ok := s.Calls[p.CallID]; ok {
			wasActive := c.Status == CallActive
			c.Status = CallEnded
			s.waiting[c.StationRef] = dropEntry(s.waiting[c.StationRef], c.CallID)
			s.compensation[c.StationRef] = dropEntry(s.compensation[c.StationRef], c.CallID)
			// 只有真正占用容量的在网通话结束才释放；被抢占/排队中的通话不计容量。
			if st, ok := s.Stations[c.StationRef]; ok && wasActive {
				st.activeCalls--
				if st.activeCalls < 0 {
					st.activeCalls = 0
				}
			}
		}

	case *domain.CompensationReplayedPayload:
		s.compensation[p.StationRef] = dropEntry(s.compensation[p.StationRef], p.CallID)

	case *domain.ManualDirectivePayload:
		s.Directives = append(s.Directives, DirectiveView{ManualDirectivePayload: *p, OccurredAt: at})
		// 即时生效的动词（FREEZE/RESUME 的查询本身从指令流推导；以下两个动词改变结构性状态）。
		if p.Kind == domain.ManualForce {
			if hasDecisionVerb(p.Decision, "UNPIN_ROUTE") {
				route := decisionArg(p.Decision, "UNPIN_ROUTE")
				delete(s.manualPins, route)
			}
			if hasDecisionVerb(p.Decision, "PROTECT_CALL") {
				if c, ok := s.Calls[decisionArg(p.Decision, "PROTECT_CALL")]; ok {
					c.Protected = true
				}
			}
		}

	case *domain.ReconResultPayload:
		s.Recons[p.ReconID] = &ReconView{ReconResultPayload: *p, OccurredAt: at}
	}
}

func (s *State) ensureTask(ref string) *taskState {
	t, ok := s.Tasks[ref]
	if !ok {
		t = &taskState{Ref: ref, Phase: domain.PhaseStandby}
		s.Tasks[ref] = t
	}
	return t
}

func appendQueue(queue []*QueueEntry, e *QueueEntry) []*QueueEntry {
	for _, existing := range queue {
		if existing.ID == e.ID {
			return queue
		}
	}
	return append(queue, e)
}

// dropEntry 从队列中移除指定通话并压缩切片（保持先进先出顺序）。
func dropEntry(queue []*QueueEntry, callID string) []*QueueEntry {
	kept := queue[:0]
	for _, e := range queue {
		if e != nil && e.CallID != callID {
			kept = append(kept, e)
		}
	}
	// 清除被截掉位置的引用，避免内存泄漏。
	for i := len(kept); i < len(queue); i++ {
		queue[i] = nil
	}
	return kept
}

// SortedPhaseHistory 返回按发生时间排序的阶段记录，供快照与还原使用。
func SortedPhaseHistory(records []PhaseRecord) []PhaseRecord {
	out := make([]PhaseRecord, len(records))
	copy(out, records)
	sort.SliceStable(out, func(i, j int) bool { return out[i].OccurredAt.Before(out[j].OccurredAt) })
	return out
}
