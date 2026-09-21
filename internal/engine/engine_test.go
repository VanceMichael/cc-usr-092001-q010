package engine_test

import (
	"strings"
	"testing"
	"time"

	"example.com/batch-092001-q010/internal/command"
	"example.com/batch-092001-q010/internal/domain"
	"example.com/batch-092001-q010/internal/engine"
	"example.com/batch-092001-q010/internal/store"
)

// fixture 封装一个内存控制器与每个来源的序号计数。
type fixture struct {
	t     *testing.T
	e     *engine.Engine
	log   store.Log
	seqs  map[string]int64
	clock time.Time
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	log := store.NewMemoryLog()
	e, err := engine.New(log)
	if err != nil {
		t.Fatalf("初始化引擎失败: %v", err)
	}
	return &fixture{
		t: t, e: e, log: log, seqs: map[string]int64{},
		clock: time.Date(2026, 9, 21, 8, 0, 0, 0, time.FixedZone("CST", 8*3600)),
	}
}

func (f *fixture) at(minute int) time.Time {
	return f.clock.Add(time.Duration(minute) * time.Minute)
}

// submit 以给定来源的下一个连续序号提交事件。
func (f *fixture) submit(eventID, subject, typ, source string, at time.Time, payload any,
	opts command.Options) *engine.Result {
	f.t.Helper()
	f.seqs[source]++
	env, err := command.Build(eventID, subject, typ, source, f.seqs[source], at, payload, opts)
	if err != nil {
		f.t.Fatalf("构造事件 %s 失败: %v", eventID, err)
	}
	result, err := f.e.Ingest(env)
	if err != nil {
		f.t.Fatalf("摄入事件 %s 被拒绝: %v", eventID, err)
	}
	return result
}

// submitSeq 用显式序号提交（用于乱序/补传/重试测试）。
func (f *fixture) submitSeq(seq int64, eventID, subject, typ, source string, at time.Time, payload any) (*engine.Result, error) {
	f.t.Helper()
	env, err := command.Build(eventID, subject, typ, source, seq, at, payload, command.Options{})
	if err != nil {
		f.t.Fatalf("构造事件 %s 失败: %v", eventID, err)
	}
	return f.e.Ingest(env)
}

// resend 重放同一信封（中心重试）。
func (f *fixture) resend(env domain.Envelope) (*engine.Result, error) {
	return f.e.Ingest(env)
}

// seedMission 建立一个已进入 NETWORK_UP 阶段的标准任务：区域 v1、无人机、
// 许可、航线、签署计划、激活、阶段推进 STANDBY→RELOCATE→NETWORK_UP。
func (f *fixture) seedMission() {
	f.submit("EV-AREA-1", "AREA-YB", domain.TypeAreaVersioned, "hq", f.at(0),
		domain.AreaVersionedPayload{AreaID: "AREA-YB", Version: 1, Name: "盐边受灾区域"},
		command.Options{Operator: domain.RoleCommander})
	f.submit("EV-DRONE-1", "DRONE-YL-01", domain.TypeDroneRegistered, "logistics", f.at(1),
		domain.DroneRegisteredPayload{
			DroneSerial: "DRONE-YL-01", ModelRef: "YL-WING-2", ReconCapability: true,
			StationCapability: true, MaxFlightMinutes: 240, FuelKind: "HYBRID",
		}, command.Options{})
	f.submit("EV-CLR-1", "CLR-01", domain.TypeClearanceGranted, "airspace", f.at(2),
		domain.ClearanceGrantedPayload{
			ClearanceID: "CLR-01", DroneSerial: "DRONE-YL-01", AirspaceRef: "AIR-YB-A",
			ValidFrom: f.at(-60).Format(time.RFC3339), ValidUntil: f.at(720).Format(time.RFC3339),
			GrantedBy: "AIRSPACE-DESK-1",
		}, command.Options{Operator: domain.RoleAirspace})
	f.submit("EV-ROUTE-1", "RT-01", domain.TypeRouteUpdated, "flightops", f.at(3),
		domain.RouteUpdatedPayload{
			RouteRef: "RT-01", PlanID: "PLAN-A", ChangedBy: "FLIGHT-OPS-1",
			Waypoints: []domain.Waypoint{
				{Seq: 1, Latitude: 26.9, Longitude: 101.5, AltitudeM: 1200},
				{Seq: 2, Latitude: 27.0, Longitude: 101.6, AltitudeM: 1200},
			},
		}, command.Options{Operator: domain.RoleFlightOps})
	f.submit("EV-PLAN-A", "PLAN-A", domain.TypePlanSigned, "hq", f.at(4),
		domain.PlanSignedPayload{
			PlanID: "PLAN-A", TaskRef: "TASK-YB-01", AreaID: "AREA-YB", AreaVersion: 1,
			DroneSerials: []string{"DRONE-YL-01"}, RouteRef: "RT-01", StationRef: "STN-01",
		}, command.Options{Operator: domain.RoleCommander})
	f.submit("EV-PLAN-A-ACT", "PLAN-A", domain.TypePlanActivated, "hq", f.at(5),
		domain.PlanActivatedPayload{PlanID: "PLAN-A", TaskRef: "TASK-YB-01", ActivatedBy: "CMD-1"},
		command.Options{Operator: domain.RoleCommander})
	f.submit("EV-STN-1", "STN-01", domain.TypeStationCoverage, "network", f.at(6),
		domain.StationCoveragePayload{
			StationRef: "STN-01", DroneSerial: "DRONE-YL-01",
			Latitude: 27.0, Longitude: 101.6, RadiusM: 5000, Capacity: 2, Used: 0, Active: true,
		}, command.Options{DeviceSerial: "DRONE-YL-01"})
	f.phase("EV-PHASE-1", "", domain.PhaseStandby, domain.PhaseRelocate, f.at(7), "起飞转场")
	f.phase("EV-PHASE-2", "EV-PHASE-1", domain.PhaseRelocate, domain.PhaseNetworkUp, f.at(20), "抵达目标空域建网")
}

func (f *fixture) phase(eventID, idempotencyHint string, from, to string, at time.Time, reason string) {
	f.t.Helper()
	_ = idempotencyHint
	f.submit(eventID, "TASK-YB-01", domain.TypeTaskPhaseChanged, "flightops", at,
		domain.TaskPhaseChangedPayload{
			TaskRef: "TASK-YB-01", FromPhase: from, ToPhase: to,
			DroneSerial: "DRONE-YL-01", Reason: reason, PlanID: "PLAN-A",
		}, command.Options{DeviceSerial: "DRONE-YL-01", Operator: "FLIGHT-OPS-1"})
}

// ---------- 需求一：断链、补传、重试、遥测都不得制造虚假飞行阶段 ----------

func TestIdempotentRetryDoesNotCreatePhase(t *testing.T) {
	f := newFixture(t)
	f.seedMission()
	before := f.e.Snapshot().EventCount

	// 遥测上报位置，绝不改变阶段。
	tel := domain.DroneTelemetryPayload{
		DroneSerial: "DRONE-YL-01", Latitude: 27.01, Longitude: 101.61,
		AltitudeM: 1200, PlanID: "PLAN-A", WaypointSeq: 2,
	}
	r := f.submit("EV-TEL-1", "DRONE-YL-01", domain.TypeDroneTelemetry, "drone-01", f.at(25), tel,
		command.Options{DeviceSerial: "DRONE-YL-01"})
	if r.Duplicate || r.Buffered {
		t.Fatal("首条遥测应正常生效")
	}
	// 中心重试：同一 event_id 原样重发，必须幂等且不产生任何派生事实。
	env, _ := command.Build("EV-TEL-1", "DRONE-YL-01", domain.TypeDroneTelemetry, "drone-01", 1,
		f.at(25), tel, command.Options{DeviceSerial: "DRONE-YL-01"})
	for i := 0; i < 3; i++ {
		rr, err := f.resend(env)
		if err != nil || !rr.Duplicate {
			t.Fatalf("第 %d 次重复遥测必须幂等，got=%+v err=%v", i, rr, err)
		}
	}
	snap := f.e.Snapshot()
	if snap.EventCount != before+1 {
		t.Fatalf("遥测重试不得追加事件：before=%d after=%d", before, snap.EventCount)
	}
	if task := snap.Tasks["TASK-YB-01"]; task.Phase != domain.PhaseNetworkUp {
		t.Fatalf("遥测不得改变阶段，got=%s", task.Phase)
	}
	if d := snap.Drones["DRONE-YL-01"]; d.Telemetry == nil || d.Telemetry.Longitude != 101.61 {
		t.Fatal("遥测位置未更新")
	}
}

func TestOfflineGapBufferingPreservesOrderAndPhase(t *testing.T) {
	f := newFixture(t)
	f.seedMission()
	tel := func(seq int64, id string, lon float64, minute int) (domain.Envelope, error) {
		env, err := command.Build(id, "DRONE-YL-01", domain.TypeDroneTelemetry, "drone-gap",
			seq, f.at(minute), domain.DroneTelemetryPayload{
				DroneSerial: "DRONE-YL-01", Latitude: 27.0, Longitude: lon,
				AltitudeM: 1200, PlanID: "PLAN-A", WaypointSeq: int(seq),
			}, command.Options{DeviceSerial: "DRONE-YL-01"})
		return env, err
	}

	// 链路中断期间先到达 seq 3 和 seq 2，必须缓冲，不得提前生效。
	env3, _ := tel(3, "EV-GAP-3", 101.63, 33)
	r3, err := f.e.Ingest(env3)
	if err != nil || !r3.Buffered {
		t.Fatalf("缺口序号 3 应缓冲，got=%+v err=%v", r3, err)
	}
	env2, _ := tel(2, "EV-GAP-2", 101.62, 32)
	r2, err := f.e.Ingest(env2)
	if err != nil || !r2.Buffered {
		t.Fatalf("缺口序号 2 应缓冲，got=%+v err=%v", r2, err)
	}
	if d := f.e.Snapshot().Drones["DRONE-YL-01"]; d.Telemetry != nil && d.Telemetry.WaypointSeq >= 2 {
		t.Fatal("缓冲事件不得提前投影到状态")
	}

	// 离线补传 seq 1，三条按 1→2→3 顺序释放。
	env1, _ := tel(1, "EV-GAP-1", 101.61, 31)
	r1, err := f.e.Ingest(env1)
	if err != nil || r1.Buffered || r1.Duplicate {
		t.Fatalf("补齐缺口后应立即按序释放，got=%+v err=%v", r1, err)
	}
	if d := f.e.Snapshot().Drones["DRONE-YL-01"]; d.Telemetry == nil || d.Telemetry.WaypointSeq != 3 {
		t.Fatal("释放后最新位置应为 seq 3")
	}
	events, _ := f.e.Events(0)
	var gapOrder []int64
	for _, ev := range events {
		if ev.Envelope.Source == "drone-gap" {
			gapOrder = append(gapOrder, ev.Envelope.SourceSequence)
		}
	}
	if len(gapOrder) != 3 || gapOrder[0] != 1 || gapOrder[1] != 2 || gapOrder[2] != 3 {
		t.Fatalf("补传事件必须按序号 1,2,3 落盘，got=%v", gapOrder)
	}
	if task := f.e.Snapshot().Tasks["TASK-YB-01"]; task.Phase != domain.PhaseNetworkUp {
		t.Fatal("补传不得改变阶段")
	}

	// 迟到的旧序号（新 event_id）必须拒绝，不能覆盖事实。
	envStale, _ := tel(2, "EV-GAP-2-REPLAY-NEWID", 99.99, 40)
	_, err = f.e.Ingest(envStale)
	if err == nil || !strings.Contains(err.Error(), "STALE_SEQUENCE") {
		t.Fatalf("迟到旧序号应返回 STALE_SEQUENCE，got=%v", err)
	}
	// 缓冲期间重复投递 seq 3（同一 event_id）：返回幂等，不落盘。
	rr, err := f.e.Ingest(env3)
	if err != nil || !rr.Duplicate {
		t.Fatalf("缓冲中重复事件应幂等，got=%+v err=%v", rr, err)
	}
}

func TestPhaseTransitionsRequireExplicitFacts(t *testing.T) {
	f := newFixture(t)
	f.seedMission()

	// 被拒绝的事件不消耗来源序号，以下三次尝试都用当前期望序号提交。
	badJump := domain.TaskPhaseChangedPayload{
		TaskRef: "TASK-YB-01", FromPhase: domain.PhaseNetworkUp, ToPhase: domain.PhaseWithdraw,
		DroneSerial: "DRONE-YL-01", PlanID: "PLAN-A",
	}
	// 跳级必须被拒。
	_, err := f.submitSeq(f.seqs["flightops"]+1, "EV-BAD-JUMP", "TASK-YB-01",
		domain.TypeTaskPhaseChanged, "flightops", f.at(40), badJump)
	expectRule(t, err, "跳级")

	// from_phase 与事实不符（离线补传旧阶段事件）必须被拒。
	_, err = f.submitSeq(f.seqs["flightops"]+1, "EV-BAD-FROM", "TASK-YB-01",
		domain.TypeTaskPhaseChanged, "flightops", f.at(41),
		domain.TaskPhaseChangedPayload{
			TaskRef: "TASK-YB-01", FromPhase: domain.PhaseStandby, ToPhase: domain.PhaseRelocate,
			DroneSerial: "DRONE-YL-01", PlanID: "PLAN-A",
		})
	expectRule(t, err, "过期阶段事实")

	// 无设备序号的阶段推进必须被拒（信封层）。
	good := domain.TaskPhaseChangedPayload{
		TaskRef: "TASK-YB-01", FromPhase: domain.PhaseNetworkUp, ToPhase: domain.PhaseSustain,
		DroneSerial: "DRONE-YL-01", PlanID: "PLAN-A", Reason: "持续保障",
	}
	env, _ := command.Build("EV-BAD-NOSERIAL", "TASK-YB-01", domain.TypeTaskPhaseChanged,
		"flightops-bad", 1, f.at(42), good, command.Options{Operator: "FLIGHT-OPS-1"})
	env.DeviceSerial = ""
	if _, err := f.e.Ingest(env); err == nil {
		t.Fatal("无设备序号的阶段推进必须被拒绝")
	}
	// 载荷层也必须带设备序号。
	good.DroneSerial = ""
	_, err = f.submitSeq(f.seqs["flightops"]+1, "EV-BAD-NOSERIAL2", "TASK-YB-01",
		domain.TypeTaskPhaseChanged, "flightops", f.at(43), good)
	expectRule(t, err, "载荷缺少设备序号")

	// 合法推进成功。
	f.phase("EV-PHASE-3", "", domain.PhaseNetworkUp, domain.PhaseSustain, f.at(44), "进入持续保障")
	if task := f.e.Snapshot().Tasks["TASK-YB-01"]; task.Phase != domain.PhaseSustain {
		t.Fatal("合法推进后应为 SUSTAIN")
	}
}

func expectRule(t *testing.T, err error, scene string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s：期望被规则拒绝，实际通过", scene)
	}
	var rj *engine.RejectError
	if !asReject(err, &rj) {
		t.Fatalf("%s：期望 RejectError，got=%T %v", scene, err, err)
	}
}

func asReject(err error, target **engine.RejectError) bool {
	for err != nil {
		if r, ok := err.(*engine.RejectError); ok {
			*target = r
			return true
		}
		break
	}
	return false
}

// 无许可不得起飞；人工 FORCE 豁免可放行。
func TestPhaseRequiresClearanceOrForceOverride(t *testing.T) {
	f := newFixture(t)
	// 只建区域、无人机、计划，但不发起飞许可。
	f.submit("EV-AREA-1", "AREA-YB", domain.TypeAreaVersioned, "hq", f.at(0),
		domain.AreaVersionedPayload{AreaID: "AREA-YB", Version: 1}, command.Options{Operator: domain.RoleCommander})
	f.submit("EV-DRONE-1", "DRONE-YL-01", domain.TypeDroneRegistered, "logistics", f.at(1),
		domain.DroneRegisteredPayload{DroneSerial: "DRONE-YL-01", StationCapability: true, MaxFlightMinutes: 120},
		command.Options{})
	f.submit("EV-ROUTE-1", "RT-01", domain.TypeRouteUpdated, "flightops", f.at(2),
		domain.RouteUpdatedPayload{RouteRef: "RT-01", ChangedBy: "FLIGHT-OPS-1",
			Waypoints: []domain.Waypoint{{Seq: 1, Latitude: 27, Longitude: 101.5, AltitudeM: 1000}}},
		command.Options{})
	f.submit("EV-PLAN-A", "PLAN-A", domain.TypePlanSigned, "hq", f.at(3),
		domain.PlanSignedPayload{PlanID: "PLAN-A", TaskRef: "TASK-X", AreaID: "AREA-YB", AreaVersion: 1,
			DroneSerials: []string{"DRONE-YL-01"}, RouteRef: "RT-01", StationRef: "STN-X"},
		command.Options{Operator: domain.RoleCommander})
	f.submit("EV-ACT", "PLAN-A", domain.TypePlanActivated, "hq", f.at(4),
		domain.PlanActivatedPayload{PlanID: "PLAN-A", TaskRef: "TASK-X", ActivatedBy: "CMD-1"},
		command.Options{Operator: domain.RoleCommander})

	bad := domain.TaskPhaseChangedPayload{TaskRef: "TASK-X", FromPhase: domain.PhaseStandby,
		ToPhase: domain.PhaseRelocate, DroneSerial: "DRONE-YL-01", PlanID: "PLAN-A"}
	env, _ := command.Build("EV-PHASE-NOCLR", "TASK-X", domain.TypeTaskPhaseChanged, "flightops",
		1, f.at(5), bad, command.Options{DeviceSerial: "DRONE-YL-01"})
	if _, err := f.e.Ingest(env); err == nil {
		t.Fatal("无有效许可不得进入转场")
	}

	// 指挥员 FORCE 豁免许可要求。
	f.submit("EV-FORCE-CLR", "TASK-X", domain.TypeManualDirective, "cmd", f.at(6),
		domain.ManualDirectivePayload{DirectiveID: "DIR-1", Kind: domain.ManualForce,
			IssuedBy: "CMD-1", Role: domain.RoleCommander, Scope: "TASK:TASK-X",
			Decision: "OVERRIDE_CLEARANCE:TASK-X", Reason: "紧急生命救援"},
		command.Options{Operator: domain.RoleCommander})
	env2, _ := command.Build("EV-PHASE-FORCED", "TASK-X", domain.TypeTaskPhaseChanged, "flightops",
		2, f.at(7), bad, command.Options{DeviceSerial: "DRONE-YL-01"})
	if _, err := f.e.Ingest(env2); err != nil {
		t.Fatalf("人工 FORCE 豁免后应放行: %v", err)
	}
}

// ---------- 需求二：重启后现场继续使用最后一份已签署计划 ----------

func TestRestartRebuildsStateFromLog(t *testing.T) {
	path := t.TempDir() + "/events.jsonl"
	fl, err := store.OpenFileLog(path)
	if err != nil {
		t.Fatal(err)
	}
	e1, err := engine.New(fl)
	if err != nil {
		t.Fatal(err)
	}

	// 直接用引擎跑一遍标准任务：手工提交事件（复用 fixture 思路的简化版）。
	build := func(e *engine.Engine) *fixture {
		return &fixture{t: t, e: e, log: fl, seqs: map[string]int64{}, clock: time.Date(2026, 9, 21, 8, 0, 0, 0, time.FixedZone("CST", 8*3600))}
	}
	f := build(e1)
	f.seedMission()
	countBefore := f.e.Snapshot().EventCount
	if err := fl.Close(); err != nil {
		t.Fatal(err)
	}

	// 中心重启：重新打开同一日志，完整重建。
	fl2, err := store.OpenFileLog(path)
	if err != nil {
		t.Fatalf("重启打开日志失败: %v", err)
	}
	e2, err := engine.New(fl2)
	if err != nil {
		t.Fatalf("重启重建状态失败: %v", err)
	}
	snap := e2.Snapshot()
	if snap.EventCount != countBefore {
		t.Fatalf("重启后事件数应一致：%d vs %d", countBefore, snap.EventCount)
	}
	task := snap.Tasks["TASK-YB-01"]
	if task.Phase != domain.PhaseNetworkUp || task.ActivePlanID != "PLAN-A" {
		t.Fatalf("重启后现场事实丢失：phase=%s plan=%s", task.Phase, task.ActivePlanID)
	}
	plan := snap.Plans["PLAN-A"]
	if !plan.Activated || plan.AreaVersion != 1 {
		t.Fatal("重启后最后签署计划不可用")
	}
	if len(task.History) != 2 {
		t.Fatalf("阶段历史必须完整恢复，got=%d", len(task.History))
	}
}

func TestTamperedLogDetected(t *testing.T) {
	path := t.TempDir() + "/events.jsonl"
	fl, err := store.OpenFileLog(path)
	if err != nil {
		t.Fatal(err)
	}
	e, err := engine.New(fl)
	if err != nil {
		t.Fatal(err)
	}
	env, _ := command.Build("EV-1", "SUBJ-1", domain.TypeDroneRegistered, "src", 1,
		time.Date(2026, 9, 21, 8, 0, 0, 0, time.UTC),
		domain.DroneRegisteredPayload{DroneSerial: "D-1"}, command.Options{})
	if _, err := e.Ingest(env); err != nil {
		t.Fatal(err)
	}
	if err := fl.Close(); err != nil {
		t.Fatal(err)
	}
	// 篡改日志中的一个字符，链式摘要校验必须失败。
	data := readFile(t, path)
	data = strings.Replace(data, "D-1", "D-9", 1)
	writeFile(t, path, data)
	if _, err := store.OpenFileLog(path); err == nil {
		t.Fatal("被篡改的日志必须在重放时被发现并拒绝启动")
	}
}

// ---------- 需求三：受控改派与岗位确认 ----------

func (f *fixture) signPlanB() {
	f.submit("EV-PLAN-B", "PLAN-B", domain.TypePlanSigned, "hq", f.at(50),
		domain.PlanSignedPayload{
			PlanID: "PLAN-B", TaskRef: "TASK-YB-01", AreaID: "AREA-YB", AreaVersion: 1,
			DroneSerials: []string{"DRONE-YL-01"}, RouteRef: "RT-01", StationRef: "STN-01",
			RevisionOf: "PLAN-A",
		}, command.Options{Operator: domain.RoleCommander})
}

func TestCoverageDropRetaskRequiresNetworkOps(t *testing.T) {
	f := newFixture(t)
	f.seedMission()

	// 一通普通通话占满容量（capacity=2，这里先降到 1 再制造下降更直接）。
	// 上报覆盖下降：容量降为 0。
	r := f.submit("EV-COV-DROP", "STN-01", domain.TypeStationCoverage, "network", f.at(30),
		domain.StationCoveragePayload{
			StationRef: "STN-01", DroneSerial: "DRONE-YL-01", Latitude: 27, Longitude: 101.6,
			RadiusM: 5000, Capacity: 0, Used: 0, Active: true,
		}, command.Options{DeviceSerial: "DRONE-YL-01"})
	if len(r.Derived) == 0 {
		t.Fatal("覆盖下降应自动生成受控改派提案")
	}
	var retaskID string
	for _, rt := range f.e.Snapshot().Retasks {
		if rt.TriggerType == domain.TriggerCoverageDrop {
			retaskID = rt.RetaskID
			if rt.RequiredRole != domain.RoleNetworkOps || rt.Status != engine.RetaskProposed {
				t.Fatalf("覆盖下降改派应为待 NETWORK_OPS 确认，got=%+v", rt)
			}
		}
	}
	if retaskID == "" {
		t.Fatal("未找到覆盖下降改派")
	}

	// 航线已钉死：自动策略改航线必须被拒（用 flightops 来源的下一序号立即释放）。
	_, err := f.submitSeq(f.seqs["flightops"]+1, "EV-ROUTE-AUTO", "RT-01",
		domain.TypeRouteUpdated, "flightops", f.at(31),
		domain.RouteUpdatedPayload{RouteRef: "RT-01", ChangedBy: domain.RoleSystem,
			Waypoints: []domain.Waypoint{{Seq: 1, Latitude: 1, Longitude: 1, AltitudeM: 1}}})
	expectRule(t, err, "钉死航线自动变更")

	// 错误岗位确认必须被拒。
	f.seqs["airspace"]++
	wrong, _ := command.Build("EV-CONFIRM-WRONG", retaskID, domain.TypeRetaskConfirmed, "airspace",
		f.seqs["airspace"], f.at(32),
		domain.RetaskConfirmedPayload{RetaskID: retaskID, ConfirmedBy: "AS-1", Role: domain.RoleAirspace},
		command.Options{Operator: domain.RoleAirspace})
	if _, err := f.e.Ingest(wrong); err == nil {
		t.Fatal("NETWORK_OPS 的改派不能由 AIRSPACE 确认")
	}

	// 网络保障岗签署新计划并确认，新计划激活、航线解封。
	f.signPlanB()
	f.submit("EV-CONFIRM-OK", retaskID, domain.TypeRetaskConfirmed, "network", f.at(33),
		domain.RetaskConfirmedPayload{RetaskID: retaskID, ConfirmedBy: "NETOPS-1",
			Role: domain.RoleNetworkOps, PlanID: "PLAN-B"},
		command.Options{Operator: domain.RoleNetworkOps})
	snap := f.e.Snapshot()
	if snap.Tasks["TASK-YB-01"].ActivePlanID != "PLAN-B" {
		t.Fatal("确认后应激活 PLAN-B")
	}
	if snap.Retasks[retaskID].Status != engine.RetaskConfirmed {
		t.Fatal("改派状态应为 CONFIRMED")
	}
	// 确认后航线可按新计划更新。
	f.submit("EV-ROUTE-2", "RT-01", domain.TypeRouteUpdated, "flightops", f.at(34),
		domain.RouteUpdatedPayload{RouteRef: "RT-01", PlanID: "PLAN-B", ChangedBy: "FLIGHT-OPS-1",
			Waypoints: []domain.Waypoint{{Seq: 1, Latitude: 27.2, Longitude: 101.8, AltitudeM: 1300}}},
		command.Options{Operator: domain.RoleFlightOps})
}

func TestObservedRetasksMapToRequiredRoles(t *testing.T) {
	cases := []struct {
		name    string
		source  string
		typ     string
		trigger string
		role    string
		payload any
		subject string
	}{
		{"人员位置更新", "ground", domain.TypePersonSighted, domain.TriggerPersonFound,
			domain.RoleGroundTeam,
			domain.PersonSightedPayload{SightingID: "SIGHT-1", TaskRef: "TASK-YB-01",
				TeamRef: "TEAM-3", Latitude: 27.12, Longitude: 101.66, LocationRef: "LOC-7"},
			"TASK-YB-01"},
		{"空域冲突", "airspace-feed", domain.TypeAirspaceNotice, domain.TriggerAirspace,
			domain.RoleAirspace,
			domain.AirspaceNoticePayload{NoticeID: "NOTICE-1", TaskRef: "TASK-YB-01",
				AirspaceRef: "AIR-YB-A", Kind: "CLOSURE"},
			"TASK-YB-01"},
		{"次生灾害", "sensor-net", domain.TypeSecondaryDisaster, domain.TriggerSecondary,
			domain.RoleCommander,
			domain.SecondaryDisasterPayload{ObservationID: "OBS-1", TaskRef: "TASK-YB-01",
				Kind: "二次滑坡", AreaID: "AREA-YB", NewAreaVersion: 0},
			"TASK-YB-01"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			f.seedMission()
			r := f.submit("EV-TRIG-"+tc.trigger, tc.subject, tc.typ, tc.source, f.at(28), tc.payload,
				command.Options{})
			if len(r.Derived) == 0 {
				t.Fatalf("%s 应自动生成改派提案", tc.name)
			}
			pinned := false
			for _, d := range r.Derived {
				if d.Type == domain.TypeRoutePinned {
					pinned = true
				}
			}
			if !pinned {
				t.Fatalf("%s 改派提案必须同时钉死航线", tc.name)
			}
			found := false
			for _, rt := range f.e.Snapshot().Retasks {
				if rt.TriggerType == tc.trigger {
					found = true
					if rt.RequiredRole != tc.role {
						t.Fatalf("%s 要求 %s 确认，got=%s", tc.name, tc.role, rt.RequiredRole)
					}
				}
			}
			if !found {
				t.Fatalf("%s 未生成对应改派", tc.name)
			}
		})
	}
}

func TestRejectRetaskUnpinsAndKeepsOldPlan(t *testing.T) {
	f := newFixture(t)
	f.seedMission()
	f.submit("EV-SIGHT", "TASK-YB-01", domain.TypePersonSighted, "ground", f.at(28),
		domain.PersonSightedPayload{SightingID: "S-1", TaskRef: "TASK-YB-01", TeamRef: "TEAM-3",
			Latitude: 27.1, Longitude: 101.7}, command.Options{Operator: domain.RoleGroundTeam})
	var retaskID string
	for id, rt := range f.e.Snapshot().Retasks {
		if rt.TriggerType == domain.TriggerPersonFound {
			retaskID = id
		}
	}
	f.submit("EV-REJECT", retaskID, domain.TypeRetaskRejected, "ground", f.at(29),
		domain.RetaskRejectedPayload{RetaskID: retaskID, RejectedBy: "GT-LEAD-3",
			Role: domain.RoleGroundTeam, Reason: "位置经核实为误报"},
		command.Options{Operator: domain.RoleGroundTeam})
	snap := f.e.Snapshot()
	if snap.Tasks["TASK-YB-01"].ActivePlanID != "PLAN-A" {
		t.Fatal("拒绝改派后必须维持原计划")
	}
	if snap.Retasks[retaskID].Status != engine.RetaskRejected {
		t.Fatal("改派状态应为 REJECTED")
	}
}
