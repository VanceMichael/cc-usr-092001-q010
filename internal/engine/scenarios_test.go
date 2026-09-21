package engine_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"example.com/batch-092001-q010/internal/command"
	"example.com/batch-092001-q010/internal/domain"
	"example.com/batch-092001-q010/internal/engine"
	"example.com/batch-092001-q010/internal/service"
)

func readFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

// ---------- 需求四：高优先级抢占普通容量，必须留补偿队列并回放 ----------

func TestPreemptionLeavesCompensationAndReplays(t *testing.T) {
	f := newFixture(t)
	f.seedMission()

	// 两通普通通话占满容量 2。
	f.call("CALL-100", 3, f.at(25))
	f.call("CALL-101", 4, f.at(26))
	if free := f.e.Snapshot().Stations["STN-01"].FreeCapacity; free != 0 {
		t.Fatalf("容量应被占满，free=%d", free)
	}

	// 高优先级通话进入：必须抢占一通普通通话，并留下补偿条目。
	r := f.call("CALL-911", 1, f.at(27))
	var preemptID, compID string
	for _, d := range r.Derived {
		if d.Type == domain.TypeCallPreempted {
			preemptID = d.EventID
		}
	}
	if preemptID == "" {
		t.Fatal("高优先级通话必须产生抢占裁决")
	}
	snap := f.e.Snapshot()
	if snap.Calls["CALL-911"].Status != engine.CallActive {
		t.Fatal("高优先级通话应已接通")
	}
	victimActive := 0
	var victimID string
	for _, id := range []string{"CALL-100", "CALL-101"} {
		if snap.Calls[id].Status == engine.CallActive {
			victimActive++
		}
		if snap.Calls[id].Status == engine.CallPreempted {
			victimID = id
		}
	}
	if victimActive != 1 || victimID == "" {
		t.Fatal("必须恰有一通普通通话被抢占进入补偿队列")
	}
	var queue *engine.QueueView
	for i := range snap.Queues {
		if snap.Queues[i].StationRef == "STN-01" {
			queue = &snap.Queues[i]
		}
	}
	if queue == nil || len(queue.Compensation) != 1 {
		t.Fatalf("补偿队列必须留存 1 条，got=%+v", queue)
	}
	compID = queue.Compensation[0].ID
	if !strings.HasPrefix(compID, "COMP-CALL-911-") {
		t.Fatalf("补偿条目应关联双方通话，got=%s", compID)
	}

	// 又一通普通通话：容量满且无优先级，进普通等待队列。
	f.call("CALL-200", 5, f.at(28))
	snap = f.e.Snapshot()
	if snap.Calls["CALL-200"].Status != engine.CallWaiting {
		t.Fatalf("拥塞时普通通话应排队，got=%s", snap.Calls["CALL-200"].Status)
	}

	// 高优先级通话结束：释放容量，必须先回放补偿，再放普通等待。
	f.endCall("CALL-911", f.at(29))
	snap = f.e.Snapshot()
	if snap.Calls[victimID].Status != engine.CallActive {
		t.Fatalf("被抢占通话 %s 应经补偿回放重新接通", victimID)
	}
	if len(findQueue(snap.Queues, "STN-01").Compensation) != 0 {
		t.Fatal("补偿条目回放后应出队（裁决事实仍保留在事件日志中）")
	}

	// 再结束一通，普通等待 CALL-200 才按序接通（补偿优先）。
	f.endCall(victimID, f.at(30))
	snap = f.e.Snapshot()
	if snap.Calls["CALL-200"].Status != engine.CallActive {
		t.Fatalf("补偿清空后普通排队通话应接通，got=%s", snap.Calls["CALL-200"].Status)
	}
}

func findQueue(queues []engine.QueueView, station string) *engine.QueueView {
	for i := range queues {
		if queues[i].StationRef == station {
			return &queues[i]
		}
	}
	return &engine.QueueView{StationRef: station}
}

func (f *fixture) call(id string, priority int, at time.Time) *engine.Result {
	f.t.Helper()
	return f.submit("EV-"+id, id, domain.TypeCallRequested, "calls", at,
		domain.CallRequestedPayload{CallID: id, StationRef: "STN-01", TeamRef: "TEAM-1",
			Priority: priority}, command.Options{})
}

func (f *fixture) endCall(id string, at time.Time) {
	f.t.Helper()
	f.submit("EV-END-"+id, id, domain.TypeCallEnded, "calls", at,
		domain.CallEndedPayload{CallID: id}, command.Options{})
}

// 冻结期间自动抢占被禁止；恢复后排队高优先级通话按规则处理。
func TestFreezeCapacityBlocksAutomaticPreemption(t *testing.T) {
	f := newFixture(t)
	f.seedMission()
	f.call("CALL-100", 3, f.at(25))
	f.call("CALL-101", 4, f.at(26))

	// 网络保障岗冻结 STN-01 的自动抢占。
	f.submit("EV-FREEZE", "STN-01", domain.TypeManualDirective, "netops", f.at(27),
		domain.ManualDirectivePayload{DirectiveID: "DIR-FREEZE", Kind: domain.ManualFreezeCapacity,
			IssuedBy: "NETOPS-1", Role: domain.RoleNetworkOps, Scope: "STATION:STN-01",
			Reason: "关键救援通信窗口，禁止系统自动踢人"},
		command.Options{Operator: domain.RoleNetworkOps})

	r := f.call("CALL-911", 1, f.at(28))
	for _, d := range r.Derived {
		if d.Type == domain.TypeCallPreempted {
			t.Fatal("冻结期间自动策略不得抢占")
		}
	}
	snap := f.e.Snapshot()
	if snap.Calls["CALL-911"].Status != engine.CallWaiting {
		t.Fatalf("冻结期高优先级通话也应排队，got=%s", snap.Calls["CALL-911"].Status)
	}
	if q := findQueue(snap.Queues, "STN-01"); len(q.Waiting) != 1 || q.Waiting[0].Reason != engine.QueueFrozen {
		t.Fatalf("排队原因必须记录为 FROZEN，got=%+v", q.Waiting)
	}
	if snap.Calls["CALL-100"].Status != engine.CallActive || snap.Calls["CALL-101"].Status != engine.CallActive {
		t.Fatal("冻结期原有通话必须保持接通")
	}

	// 解除冻结，容量仍满：高优先级通话现在可以抢占。
	f.submit("EV-RESUME", "STN-01", domain.TypeManualDirective, "netops", f.at(29),
		domain.ManualDirectivePayload{DirectiveID: "DIR-RESUME", Kind: domain.ManualResumeCapacity,
			IssuedBy: "NETOPS-1", Role: domain.RoleNetworkOps, Scope: "STATION:STN-01"},
		command.Options{Operator: domain.RoleNetworkOps})
	snap = f.e.Snapshot()
	if snap.Calls["CALL-911"].Status != engine.CallActive {
		// 恢复瞬间容量仍满，队列首位的高优先级应触发抢占。
		t.Fatalf("恢复后高优先级通话应抢占接通，got=%s", snap.Calls["CALL-911"].Status)
	}
}

// 人工 FORCE 保护的通话不得被自动抢占；FORCE 还能强制接入一通无容量通话。
func TestForceDirectivesOverrideAutomaticPolicy(t *testing.T) {
	f := newFixture(t)
	f.seedMission()
	f.call("CALL-100", 3, f.at(25))
	f.call("CALL-101", 4, f.at(26))

	// 指挥员保护 CALL-100：自动抢占必须跳过它。
	f.submit("EV-PROTECT", "CALL-100", domain.TypeManualDirective, "cmd", f.at(27),
		domain.ManualDirectivePayload{DirectiveID: "DIR-PROTECT", Kind: domain.ManualForce,
			IssuedBy: "CMD-1", Role: domain.RoleCommander, Scope: "CALL:CALL-100",
			Decision: "PROTECT_CALL:CALL-100", Reason: "与被困人员保持联络"},
		command.Options{Operator: domain.RoleCommander})

	f.call("CALL-911", 1, f.at(28))
	snap := f.e.Snapshot()
	if snap.Calls["CALL-100"].Status != engine.CallActive {
		t.Fatal("受 FORCE 保护的通话不得被抢占")
	}
	if snap.Calls["CALL-101"].Status != engine.CallPreempted {
		t.Fatal("抢占必须选择未受保护的普通通话 CALL-101")
	}

	// 再次占满后，人工 FORCE 强制接入一通通话，不受容量限制。
	f.endCall("CALL-101", f.at(29)) // 补偿回放使 101 回来 → 又满
	snap = f.e.Snapshot()
	if snap.Stations["STN-01"].FreeCapacity != 0 {
		t.Fatal("容量应再次占满")
	}
	f.submit("EV-FORCE-ADMIT", "CALL-300", domain.TypeCallRequested, "cmd-terminal", f.at(30),
		domain.CallRequestedPayload{CallID: "CALL-300", StationRef: "STN-01", Priority: 5},
		command.Options{Operator: domain.RoleCommander})
	f.submit("EV-FORCE", "CALL-300", domain.TypeManualDirective, "cmd", f.at(31),
		domain.ManualDirectivePayload{DirectiveID: "DIR-FORCE", Kind: domain.ManualForce,
			IssuedBy: "CMD-1", Role: domain.RoleCommander, Scope: "CALL:CALL-300",
			Decision: "ADMIT_CALL:CALL-300", Reason: "指挥部专线，立即接通"},
		command.Options{Operator: domain.RoleCommander})
	snap = f.e.Snapshot()
	c300 := snap.Calls["CALL-300"]
	if c300 == nil || !c300.ForcedAdmit || c300.Status != engine.CallActive {
		t.Fatalf("FORCE 必须强制接入 CALL-300 且留痕，got=%+v", c300)
	}
	// 强制接入不得把别的通话挤走（允许超出常规容量，这是人工决定的后果）。
	if snap.Calls["CALL-100"].Status != engine.CallActive {
		t.Fatal("强制接入不得抢占受保护通话")
	}
}

// ---------- 需求五：事后还原 ----------

func TestReconstructCallRestoresPositionCoveragePreemptionChain(t *testing.T) {
	f := newFixture(t)
	f.seedMission()

	// 抢占发生前的位置与覆盖事实。
	f.submit("EV-TEL-A", "DRONE-YL-01", domain.TypeDroneTelemetry, "drone-01", f.at(24),
		domain.DroneTelemetryPayload{DroneSerial: "DRONE-YL-01", Latitude: 27.01, Longitude: 101.61,
			AltitudeM: 1250, PlanID: "PLAN-A", WaypointSeq: 2},
		command.Options{DeviceSerial: "DRONE-YL-01"})
	f.call("CALL-100", 3, f.at(25))
	f.call("CALL-101", 4, f.at(26))
	f.call("CALL-911", 1, f.at(27)) // 抢占一通普通通话

	rec, err := f.e.ReconstructCall("CALL-911")
	if err != nil {
		t.Fatal(err)
	}
	pos, ok := rec.FlightPositions["DRONE-YL-01"]
	if !ok || pos.Longitude != 101.61 || pos.PlanID != "PLAN-A" {
		t.Fatalf("应还原当时飞行位置与锚定计划: %+v", rec.FlightPositions)
	}
	cov, ok := rec.Coverage["STN-01"]
	if !ok || cov.Capacity != 2 || !cov.Active {
		t.Fatalf("应还原当时覆盖能力: %+v", rec.Coverage)
	}
	if len(rec.Preemptions) != 1 || rec.Preemptions[0].PriorityCallID != "CALL-911" {
		t.Fatalf("应还原抢占决定: %+v", rec.Preemptions)
	}
	if rec.Preemptions[0].DecidedBy != domain.RoleSystem {
		t.Fatal("抢占裁决责任方应为 SYSTEM，且可被责任链追溯")
	}
	if rec.Call == nil || rec.Call.CallID != "CALL-911" {
		t.Fatal("还原结果应包含通话本体")
	}
	types := map[string]bool{}
	for _, link := range rec.Chain {
		types[link.Type] = true
		if link.EventID == "" || link.OccurredAt == "" {
			t.Fatal("责任链每一环都必须有事件标识与发生时间")
		}
	}
	for _, want := range []string{domain.TypeCallRequested, domain.TypeCallPreempted, domain.TypeCallAdmitted} {
		if !types[want] {
			t.Fatalf("责任链缺少 %s，实际 %v", want, types)
		}
	}
}

func TestReconstructReconRestoresPlanAndChain(t *testing.T) {
	f := newFixture(t)
	f.seedMission()
	// 侦察发生前的位置事实（锚定 PLAN-A）。
	f.submit("EV-TEL-R", "DRONE-YL-01", domain.TypeDroneTelemetry, "drone-01", f.at(34),
		domain.DroneTelemetryPayload{DroneSerial: "DRONE-YL-01", Latitude: 27.04, Longitude: 101.64,
			AltitudeM: 1240, PlanID: "PLAN-A", WaypointSeq: 2},
		command.Options{DeviceSerial: "DRONE-YL-01"})
	f.submit("EV-RECON-1", "RECON-1", domain.TypeReconResult, "drone-01", f.at(35),
		domain.ReconResultPayload{
			ReconID: "RECON-1", DroneSerial: "DRONE-YL-01", PlanID: "PLAN-A",
			Latitude: 27.05, Longitude: 101.65,
			FindingRef: "FIND-REF-9", FindingDigest: "sha256:abcdef",
		}, command.Options{DeviceSerial: "DRONE-YL-01"})

	rec, err := f.e.ReconstructRecon("RECON-1")
	if err != nil {
		t.Fatal(err)
	}
	if rec.Recon == nil || rec.Recon.FindingRef != "FIND-REF-9" {
		t.Fatal("应还原侦察成果本体")
	}
	if rec.ActivePlan == nil || rec.ActivePlan.PlanID != "PLAN-A" {
		t.Fatal("应还原当时执行的已签署计划")
	}
	if rec.TaskPhase != domain.PhaseNetworkUp {
		t.Fatalf("应还原当时飞行阶段 NETWORK_UP，got=%s", rec.TaskPhase)
	}
	if pos := rec.FlightPositions["DRONE-YL-01"]; pos.PlanID != "PLAN-A" {
		t.Fatal("应还原当时飞行位置")
	}
	chainTypes := map[string]bool{}
	for _, link := range rec.Chain {
		chainTypes[link.Type] = true
	}
	for _, want := range []string{domain.TypePlanSigned, domain.TypePlanActivated, domain.TypeTaskPhaseChanged} {
		if !chainTypes[want] {
			t.Fatalf("侦察责任链缺少 %s，实际 %v", want, chainTypes)
		}
	}
}

// ---------- HTTP 边界冒烟 ----------

func TestHTTPIngestDuplicateAndReconstruct(t *testing.T) {
	f := newFixture(t)
	srv := httptest.NewServer(service.NewServer(f.e).Handler())
	defer srv.Close()

	// 健康检查。
	resp, err := http.Get(srv.URL + "/health")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), "ok") {
		t.Fatalf("健康检查异常: %d %s", resp.StatusCode, body)
	}

	// 直接通过 HTTP 建立最小事实：登记无人机（区域版本必须先于计划）。
	post := func(env domain.Envelope) (int, map[string]any) {
		raw, _ := json.Marshal(env)
		rs, err := http.Post(srv.URL+"/v1/events/ingest", "application/json", bytes.NewReader(raw))
		if err != nil {
			t.Fatal(err)
		}
		defer rs.Body.Close()
		var out map[string]any
		_ = json.NewDecoder(rs.Body).Decode(&out)
		return rs.StatusCode, out
	}

	areaEnv, _ := command.Build("H-AREA", "AREA-H", domain.TypeAreaVersioned, "hq-http", 1,
		time.Date(2026, 9, 21, 8, 0, 0, 0, time.UTC),
		domain.AreaVersionedPayload{AreaID: "AREA-H", Version: 1}, command.Options{Operator: domain.RoleCommander})
	if code, _ := post(areaEnv); code != http.StatusOK {
		t.Fatalf("区域事件应 200，got=%d", code)
	}
	// 重复投递：仍 200 且标记 duplicate。
	if code, out := post(areaEnv); code != http.StatusOK || out["duplicate"] != true {
		t.Fatalf("重复事件应幂等 200 duplicate=true，got=%d %v", code, out)
	}

	// 缺口事件：202 buffered。
	gapEnv, _ := command.Build("H-GAP", "D-H", domain.TypeDroneRegistered, "src-gap", 5,
		time.Date(2026, 9, 21, 8, 5, 0, 0, time.UTC),
		domain.DroneRegisteredPayload{DroneSerial: "D-H"}, command.Options{})
	if code, out := post(gapEnv); code != http.StatusAccepted || out["buffered"] != true {
		t.Fatalf("缺口事件应 202 buffered=true，got=%d %v", code, out)
	}

	// 非法事件（载荷摘要不符）：400。
	bad := areaEnv
	bad.EventID = "H-BAD"
	bad.SourceSequence = 2
	bad.PayloadDigest = "sha256:deadbeef"
	if code, _ := post(bad); code != http.StatusBadRequest {
		t.Fatalf("摘要不符应 400，got=%d", code)
	}

	// 状态视图可读取。
	rs, err := http.Get(srv.URL + "/v1/state")
	if err != nil {
		t.Fatal(err)
	}
	defer rs.Body.Close()
	if rs.StatusCode != http.StatusOK {
		t.Fatalf("状态查询应 200，got=%d", rs.StatusCode)
	}
}
