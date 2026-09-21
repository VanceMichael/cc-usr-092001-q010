package engine

import (
	"time"

	"example.com/batch-092001-q010/internal/domain"
	"example.com/batch-092001-q010/internal/store"
)

// 以下 DTO 把投影中不可序列化的 time.Time 与内部结构转成对外快照。

type AreaDTO struct {
	Latest  domain.AreaVersionedPayload   `json:"latest"`
	History []domain.AreaVersionedPayload `json:"history"`
}

type DroneDTO struct {
	Registration domain.DroneRegisteredPayload   `json:"registration"`
	Telemetry    *domain.DroneTelemetryPayload   `json:"telemetry,omitempty"`
	TelemetryAt  string                          `json:"telemetry_at,omitempty"`
	Fuel         *domain.DroneFuelBatteryPayload `json:"fuel,omitempty"`
	FuelAt       string                          `json:"fuel_at,omitempty"`
}

type ClearanceDTO struct {
	domain.ClearanceGrantedPayload
	GrantedAt string `json:"grant_at"`
}

type PlanDTO struct {
	domain.PlanSignedPayload
	SignedAt    string `json:"signed_at"`
	Activated   bool   `json:"activated"`
	ActivatedAt string `json:"activated_at,omitempty"`
	ActivatedBy string `json:"activated_by,omitempty"`
}

type PhaseRecordDTO struct {
	FromPhase   string `json:"from_phase"`
	ToPhase     string `json:"to_phase"`
	DroneSerial string `json:"drone_serial"`
	Reason      string `json:"reason"`
	PlanID      string `json:"plan_id,omitempty"`
	OccurredAt  string `json:"occurred_at"`
	EventID     string `json:"event_id"`
}

type TaskDTO struct {
	Ref          string           `json:"task_ref"`
	Phase        string           `json:"phase"`
	ActivePlanID string           `json:"active_plan_id,omitempty"`
	History      []PhaseRecordDTO `json:"history"`
}

type StationDTO struct {
	domain.StationCoveragePayload
	UpdatedAt    string `json:"updated_at"`
	ActiveCalls  int    `json:"active_calls"`
	FreeCapacity int    `json:"free_capacity"`
}

type RetaskDTO struct {
	domain.RetaskProposedPayload
	Status       string `json:"status"`
	ProposedAt   string `json:"proposed_at"`
	ConfirmedAt  string `json:"confirmed_at,omitempty"`
	ConfirmedBy  string `json:"confirmed_by,omitempty"`
	RejectedBy   string `json:"rejected_by,omitempty"`
	RejectReason string `json:"reject_reason,omitempty"`
}

type ReconDTO struct {
	domain.ReconResultPayload
	OccurredAt string `json:"occurred_at"`
}

type DirectiveDTO struct {
	domain.ManualDirectivePayload
	OccurredAt string `json:"occurred_at"`
}

// QueueView 对外展示某基站的两条队列。
type QueueView struct {
	StationRef   string       `json:"station_ref"`
	Compensation []QueueEntry `json:"compensation"`
	Waiting      []QueueEntry `json:"waiting"`
}

// Snapshot 是某一时刻控制器全部稳定事实的只读快照。
type Snapshot struct {
	TakenAt    string                                    `json:"taken_at"`
	EventCount int64                                     `json:"event_count"`
	Areas      map[string]AreaDTO                        `json:"areas"`
	Drones     map[string]DroneDTO                       `json:"drones"`
	Clearances map[string]ClearanceDTO                   `json:"clearances"`
	Plans      map[string]PlanDTO                        `json:"plans"`
	Tasks      map[string]TaskDTO                        `json:"tasks"`
	Routes     map[string]domain.RouteUpdatedPayload     `json:"routes"`
	Stations   map[string]StationDTO                     `json:"stations"`
	Demands    map[string]domain.GroundTeamDemandPayload `json:"demands"`
	Priorities map[string]int                            `json:"priorities"`
	Retasks    map[string]RetaskDTO                      `json:"retasks"`
	Calls      map[string]*CallView                      `json:"calls"`
	Recons     map[string]ReconDTO                       `json:"recons"`
	Directives []DirectiveDTO                            `json:"directives"`
	Queues     []QueueView                               `json:"queues"`
}

func fmtTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339)
}

// Snapshot 返回当前状态的序列化副本。
func (e *Engine) Snapshot() Snapshot {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.state.snapshot(e.log.Len())
}

func (s *State) snapshot(eventCount int64) Snapshot {
	snap := Snapshot{
		TakenAt:    time.Now().Format(time.RFC3339),
		EventCount: eventCount,
		Areas:      map[string]AreaDTO{},
		Drones:     map[string]DroneDTO{},
		Clearances: map[string]ClearanceDTO{},
		Plans:      map[string]PlanDTO{},
		Tasks:      map[string]TaskDTO{},
		Routes:     map[string]domain.RouteUpdatedPayload{},
		Stations:   map[string]StationDTO{},
		Demands:    map[string]domain.GroundTeamDemandPayload{},
		Priorities: map[string]int{},
		Retasks:    map[string]RetaskDTO{},
		Calls:      map[string]*CallView{},
		Recons:     map[string]ReconDTO{},
	}
	for id, a := range s.Areas {
		snap.Areas[id] = AreaDTO{Latest: a.Latest, History: a.History}
	}
	for id, d := range s.Drones {
		snap.Drones[id] = DroneDTO{
			Registration: d.Registration,
			Telemetry:    d.Telemetry,
			TelemetryAt:  fmtTime(d.TelemetryAt),
			Fuel:         d.Fuel,
			FuelAt:       fmtTime(d.FuelAt),
		}
	}
	for id, c := range s.Clearances {
		snap.Clearances[id] = ClearanceDTO{ClearanceGrantedPayload: c.ClearanceGrantedPayload, GrantedAt: fmtTime(c.GrantedAt)}
	}
	for id, p := range s.Plans {
		snap.Plans[id] = PlanDTO{
			PlanSignedPayload: p.PlanSignedPayload,
			SignedAt:          fmtTime(p.SignedAt),
			Activated:         p.Activated,
			ActivatedAt:       fmtTimeValue(p.ActivatedAt),
			ActivatedBy:       p.ActivatedBy,
		}
	}
	for id, t := range s.Tasks {
		dto := TaskDTO{Ref: t.Ref, Phase: t.Phase, ActivePlanID: t.ActivePlanID}
		for _, h := range t.PhaseHistory {
			dto.History = append(dto.History, PhaseRecordDTO{
				FromPhase: h.FromPhase, ToPhase: h.ToPhase, DroneSerial: h.DroneSerial,
				Reason: h.Reason, PlanID: h.PlanID, OccurredAt: fmtTime(h.OccurredAt),
				EventID: h.Envelope.EventID,
			})
		}
		snap.Tasks[id] = dto
	}
	for id, r := range s.Routes {
		snap.Routes[id] = domain.RouteUpdatedPayload{
			RouteRef: r.Ref, Waypoints: r.Waypoints, ChangedBy: r.UpdatedBy,
		}
	}
	for id, st := range s.Stations {
		snap.Stations[id] = StationDTO{
			StationCoveragePayload: st.StationCoveragePayload,
			UpdatedAt:              fmtTime(st.UpdatedAt),
			ActiveCalls:            st.activeCalls,
			FreeCapacity:           s.FreeCapacity(id),
		}
	}
	for id, d := range s.Demands {
		snap.Demands[id] = d
	}
	for id, p := range s.Priorities {
		snap.Priorities[id] = p
	}
	for id, r := range s.Retasks {
		snap.Retasks[id] = RetaskDTO{
			RetaskProposedPayload: r.RetaskProposedPayload,
			Status:                r.Status,
			ProposedAt:            fmtTime(r.ProposedAt),
			ConfirmedAt:           fmtTimeValue(r.ConfirmedAt),
			ConfirmedBy:           r.ConfirmedBy,
			RejectedBy:            r.RejectedBy,
			RejectReason:          r.RejectReason,
		}
	}
	for id, c := range s.Calls {
		copy := *c
		snap.Calls[id] = &copy
	}
	for id, r := range s.Recons {
		snap.Recons[id] = ReconDTO{ReconResultPayload: r.ReconResultPayload, OccurredAt: fmtTime(r.OccurredAt)}
	}
	for _, d := range s.Directives {
		snap.Directives = append(snap.Directives, DirectiveDTO{
			ManualDirectivePayload: d.ManualDirectivePayload, OccurredAt: fmtTime(d.OccurredAt),
		})
	}
	seenQueues := map[string]bool{}
	addQueue := func(stationRef string) {
		if stationRef == "" || seenQueues[stationRef] {
			return
		}
		seenQueues[stationRef] = true
		snap.Queues = append(snap.Queues, QueueView{
			StationRef:   stationRef,
			Compensation: s.CompensationQueue(stationRef),
			Waiting:      s.WaitingQueue(stationRef),
		})
	}
	for stationRef := range s.compensation {
		addQueue(stationRef)
	}
	for stationRef := range s.waiting {
		addQueue(stationRef)
	}
	return snap
}

func fmtTimeValue(t *time.Time) string {
	if t == nil {
		return ""
	}
	return t.Format(time.RFC3339)
}

// EventRecord 是事件遍历的对外记录。
type EventRecord struct {
	Offset      int64           `json:"offset"`
	ChainDigest string          `json:"chain_digest"`
	Envelope    domain.Envelope `json:"envelope"`
}

// Events 按 offset 顺序返回事件记录（最多 limit 条，0 表示全部）。
func (e *Engine) Events(limit int) ([]EventRecord, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	var out []EventRecord
	err := e.log.Replay(func(rec store.Record) error {
		out = append(out, EventRecord{Offset: rec.Offset, ChainDigest: rec.ChainDigest, Envelope: rec.Envelope})
		return nil
	})
	if err != nil {
		return nil, err
	}
	if limit > 0 && len(out) > limit {
		out = out[len(out)-limit:]
	}
	return out, nil
}
