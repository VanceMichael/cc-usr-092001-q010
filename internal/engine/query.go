package engine

import (
	"sort"
	"time"

	"example.com/batch-092001-q010/internal/domain"
	"example.com/batch-092001-q010/internal/store"
)

// Reconstruction 是事后还原结果：给定某次通话或侦察成果，还原其发生时刻
// 的飞行位置、覆盖能力、抢占裁决与完整责任链。
type Reconstruction struct {
	AsOf            string                   `json:"as_of"`
	FlightPositions map[string]FlightPoint   `json:"flight_positions"` // 设备序号 -> 当时位置
	Coverage        map[string]CoveragePoint `json:"coverage"`         // 基站 -> 当时覆盖能力
	Preemptions     []PreemptionFact         `json:"preemptions"`      // 截至当时的抢占裁决
	Call            *CallView                `json:"call,omitempty"`
	Recon           *ReconView               `json:"recon,omitempty"`
	Chain           []ChainLink              `json:"chain"` // 责任链（按时间）
	ActivePlan      *PlanView                `json:"active_plan,omitempty"`
	TaskPhase       string                   `json:"task_phase,omitempty"`
}

// FlightPoint 是某时刻无人机的位置事实。
type FlightPoint struct {
	DroneSerial string  `json:"drone_serial"`
	Latitude    float64 `json:"latitude"`
	Longitude   float64 `json:"longitude"`
	AltitudeM   float64 `json:"altitude_m"`
	PlanID      string  `json:"plan_id,omitempty"`
	WaypointSeq int     `json:"waypoint_seq,omitempty"`
	At          string  `json:"at"`
}

// CoveragePoint 是某时刻基站覆盖能力事实。
type CoveragePoint struct {
	StationRef  string  `json:"station_ref"`
	DroneSerial string  `json:"drone_serial"`
	Latitude    float64 `json:"latitude"`
	Longitude   float64 `json:"longitude"`
	RadiusM     float64 `json:"radius_m"`
	Capacity    int     `json:"capacity"`
	Used        int     `json:"used"`
	Active      bool    `json:"active"`
	At          string  `json:"at"`
}

// PreemptionFact 是一次抢占裁决的完整留痕。
type PreemptionFact struct {
	StationRef      string `json:"station_ref"`
	PriorityCallID  string `json:"priority_call_id"`
	PreemptedCallID string `json:"preempted_call_id"`
	CompensationID  string `json:"compensation_id"`
	DecidedBy       string `json:"decided_by"`
	At              string `json:"at"`
	EventID         string `json:"event_id"`
}

// ChainLink 是责任链上的一环：谁（岗位/系统）在什么时间基于哪个事件做了什么。
type ChainLink struct {
	EventID      string `json:"event_id"`
	Type         string `json:"type"`
	Operator     string `json:"operator"`
	DeviceSerial string `json:"device_serial,omitempty"`
	OccurredAt   string `json:"occurred_at"`
	SubjectRef   string `json:"subject_ref"`
	Offset       int64  `json:"offset"`
}

// ReconstructCall 输入某次通话标识，还原该通话建立时刻的完整现场。
func (e *Engine) ReconstructCall(callID string) (*Reconstruction, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	call, ok := e.state.Calls[callID]
	if !ok {
		return nil, &RejectError{Code: RejectRule, Reason: "通话 " + callID + " 不存在，无法还原"}
	}
	// 以通话请求时刻为还原点：当时的位置、容量、裁决都以这一刻为准。
	at := e.eventTime(e.findFirstEvent(domain.TypeCallRequested, callID))
	if at.IsZero() {
		at = time.Now()
	}
	rec := e.reconstructAsOf(at)
	rec.Call = call
	// 责任链围绕该通话及其补偿条目收集。
	rec.Chain = e.traceCallChain(callID)
	return rec, nil
}

// ReconstructRecon 输入某次侦察成果标识，还原成果发生时刻的现场与责任链。
func (e *Engine) ReconstructRecon(reconID string) (*Reconstruction, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	recon, ok := e.state.Recons[reconID]
	if !ok {
		return nil, &RejectError{Code: RejectRule, Reason: "侦察成果 " + reconID + " 不存在，无法还原"}
	}
	at := recon.OccurredAt
	rec := e.reconstructAsOf(at)
	rec.Recon = recon
	// 责任链围绕成果锚定的计划收集：签署、激活、改派、确认、阶段推进。
	if recon.PlanID != "" {
		rec.Chain = e.tracePlanChain(recon.PlanID)
		if plan, ok := recStatePlans(e.state, recon.PlanID); ok {
			rec.ActivePlan = plan
			if t := findTaskForPlan(e.state, recon.PlanID); t != "" {
				rec.TaskPhase = phaseAt(e.state, t, at)
			}
		}
	}
	return rec, nil
}

// reconstructAsOf 把日志中 occurred_at 不晚于 at 的事件按业务时间重放到一个
// 全新投影，得到"当时的事实"。同刻事件以日志顺序破平，保证确定性。
func (e *Engine) reconstructAsOf(at time.Time) *Reconstruction {
	type timed struct {
		offset int64
		env    domain.Envelope
		t      time.Time
	}
	var all []timed
	_ = e.log.Replay(func(rec store.Record) error {
		t, _ := domain.ParseTime(rec.Envelope.OccurredAt)
		if !t.After(at) {
			all = append(all, timed{offset: rec.Offset, env: rec.Envelope, t: t})
		}
		return nil
	})
	sort.SliceStable(all, func(i, j int) bool {
		if all[i].t.Equal(all[j].t) {
			return all[i].offset < all[j].offset
		}
		return all[i].t.Before(all[j].t)
	})
	past := NewState()
	for _, item := range all {
		past.Apply(item.env)
	}

	rec := &Reconstruction{
		AsOf:            at.Format(time.RFC3339),
		FlightPositions: map[string]FlightPoint{},
		Coverage:        map[string]CoveragePoint{},
	}
	for serial, d := range past.Drones {
		if d.Telemetry != nil && !d.TelemetryAt.After(at) {
			rec.FlightPositions[serial] = FlightPoint{
				DroneSerial: serial,
				Latitude:    d.Telemetry.Latitude,
				Longitude:   d.Telemetry.Longitude,
				AltitudeM:   d.Telemetry.AltitudeM,
				PlanID:      d.Telemetry.PlanID,
				WaypointSeq: d.Telemetry.WaypointSeq,
				At:          d.TelemetryAt.Format(time.RFC3339),
			}
		}
	}
	for ref, st := range past.Stations {
		if !st.UpdatedAt.After(at) {
			rec.Coverage[ref] = CoveragePoint{
				StationRef: ref, DroneSerial: st.DroneSerial,
				Latitude: st.Latitude, Longitude: st.Longitude, RadiusM: st.RadiusM,
				Capacity: st.Capacity, Used: st.Used, Active: st.Active,
				At: st.UpdatedAt.Format(time.RFC3339),
			}
		}
	}
	for _, item := range all {
		if item.env.Type != domain.TypeCallPreempted {
			continue
		}
		p, _ := domain.DecodePayload(item.env.Type, item.env.Payload)
		if pre, ok := p.(*domain.CallPreemptedPayload); ok {
			rec.Preemptions = append(rec.Preemptions, PreemptionFact{
				StationRef: pre.StationRef, PriorityCallID: pre.PriorityCallID,
				PreemptedCallID: pre.PreemptedCallID, CompensationID: pre.CompensationID,
				DecidedBy: pre.DecidedBy, At: item.env.OccurredAt, EventID: item.env.EventID,
			})
		}
	}
	sort.SliceStable(rec.Preemptions, func(i, j int) bool {
		return rec.Preemptions[i].At < rec.Preemptions[j].At
	})
	return rec
}

// traceCallChain 收集一通电话相关的全部责任环节。
func (e *Engine) traceCallChain(callID string) []ChainLink {
	compIDs := map[string]bool{}
	var links []ChainLink
	_ = e.log.Replay(func(rec store.Record) error {
		env := rec.Envelope
		related := false
		switch p := payloadPtr(env).(type) {
		case *domain.CallRequestedPayload:
			related = p.CallID == callID
		case *domain.CallQueuedPayload:
			related = p.CallID == callID
		case *domain.CallAdmittedPayload:
			related = p.CallID == callID || p.ViaCompensationID != "" && compIDs[p.ViaCompensationID]
		case *domain.CallEndedPayload:
			related = p.CallID == callID
		case *domain.CallPreemptedPayload:
			if p.PriorityCallID == callID || p.PreemptedCallID == callID {
				related = true
				compIDs[p.CompensationID] = true
			}
		case *domain.CompensationReplayedPayload:
			related = p.CallID == callID || compIDs[p.CompensationID]
		case *domain.ManualDirectivePayload:
			related = p.Kind == domain.ManualForce &&
				(decisionArg(p.Decision, "ADMIT_CALL") == callID ||
					decisionArg(p.Decision, "PROTECT_CALL") == callID)
		}
		if related {
			links = append(links, linkFrom(rec))
		}
		return nil
	})
	sortChain(links)
	return links
}

// tracePlanChain 收集一份计划相关的全部责任环节（含其改派链路）。
func (e *Engine) tracePlanChain(planID string) []ChainLink {
	var links []ChainLink
	retasks := map[string]bool{}
	_ = e.log.Replay(func(rec store.Record) error {
		env := rec.Envelope
		related := false
		switch p := payloadPtr(env).(type) {
		case *domain.PlanSignedPayload:
			related = p.PlanID == planID || p.RevisionOf == planID
		case *domain.PlanActivatedPayload:
			related = p.PlanID == planID
		case *domain.TaskPhaseChangedPayload:
			related = p.PlanID == planID
		case *domain.RouteUpdatedPayload:
			related = p.PlanID == planID
		case *domain.RetaskProposedPayload:
			if p.ProposedPlanID == planID || p.CurrentPlanID == planID {
				related = true
				retasks[p.RetaskID] = true
			}
		case *domain.RetaskConfirmedPayload:
			related = retasks[p.RetaskID]
		case *domain.RetaskRejectedPayload:
			related = retasks[p.RetaskID]
		}
		if related {
			links = append(links, linkFrom(rec))
		}
		return nil
	})
	sortChain(links)
	return links
}

func (e *Engine) findFirstEvent(eventType, subjectCallID string) domain.Envelope {
	var found domain.Envelope
	_ = e.log.Replay(func(rec store.Record) error {
		if rec.Envelope.Type == eventType {
			if p, ok := payloadPtr(rec.Envelope).(*domain.CallRequestedPayload); ok && p.CallID == subjectCallID {
				found = rec.Envelope
				return errStopReplay
			}
		}
		return nil
	})
	return found
}

func (e *Engine) eventTime(env domain.Envelope) time.Time {
	t, _ := domain.ParseTime(env.OccurredAt)
	return t
}

// payloadPtr 解码事件载荷为强类型指针（仅用于查询遍历）。
func payloadPtr(env domain.Envelope) any {
	p, _ := domain.DecodePayload(env.Type, env.Payload)
	return p
}

func linkFrom(rec store.Record) ChainLink {
	return ChainLink{
		EventID: rec.Envelope.EventID, Type: rec.Envelope.Type,
		Operator: rec.Envelope.Operator, DeviceSerial: rec.Envelope.DeviceSerial,
		OccurredAt: rec.Envelope.OccurredAt, SubjectRef: rec.Envelope.SubjectRef,
		Offset: rec.Offset,
	}
}

func sortChain(links []ChainLink) {
	sort.SliceStable(links, func(i, j int) bool {
		if links[i].OccurredAt == links[j].OccurredAt {
			return links[i].Offset < links[j].Offset
		}
		return links[i].OccurredAt < links[j].OccurredAt
	})
}

// errStopReplay 用于在查询中提前结束遍历。
var errStopReplay = stopError{}

type stopError struct{}

func (stopError) Error() string { return "replay stopped" }

// recStatePlans / findTaskForPlan / phaseAt 为还原报告补充计划与阶段信息。
func recStatePlans(s *State, planID string) (*PlanView, bool) {
	p, ok := s.Plans[planID]
	return p, ok
}

func findTaskForPlan(s *State, planID string) string {
	for ref, t := range s.Tasks {
		if t.ActivePlanID == planID {
			return ref
		}
	}
	return ""
}

func phaseAt(s *State, taskRef string, at time.Time) string {
	phase := domain.PhaseStandby
	t, ok := s.Tasks[taskRef]
	if !ok {
		return phase
	}
	for _, record := range t.PhaseHistory {
		if !record.OccurredAt.After(at) {
			phase = record.ToPhase
		}
	}
	return phase
}
