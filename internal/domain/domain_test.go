package domain

import (
	"encoding/json"
	"testing"
)

func TestCanonicalDigestIgnoresFieldOrder(t *testing.T) {
	a := map[string]any{"drone_serial": "D-1", "capacity": 2}
	b := map[string]any{"capacity": 2, "drone_serial": "D-1"}
	_, da, err := CanonicalDigest(a)
	if err != nil {
		t.Fatal(err)
	}
	_, db, err := CanonicalDigest(b)
	if err != nil {
		t.Fatal(err)
	}
	if da != db {
		t.Fatal("字段顺序不同但内容相同的载荷摘要必须一致")
	}
	var raw json.RawMessage = []byte(`{ "capacity" : 2 , "drone_serial": "D-1" }`)
	if !VerifyDigest(raw, da) {
		t.Fatal("空白差异不应影响摘要校验")
	}
}

func TestControlledVocabulary(t *testing.T) {
	if !ValidPhase(PhaseNetworkUp) || ValidPhase("FLYING") {
		t.Fatal("阶段受控词汇异常")
	}
	if PhaseRank(PhaseWithdraw)-PhaseRank(PhaseStandby) != 4 {
		t.Fatal("阶段顺序异常")
	}
	cases := map[string]string{
		TriggerCoverageDrop: RoleNetworkOps,
		TriggerAirspace:     RoleAirspace,
		TriggerPersonFound:  RoleGroundTeam,
		TriggerSecondary:    RoleCommander,
	}
	for trigger, role := range cases {
		if RequiredRoleForTrigger(trigger) != role {
			t.Fatalf("触发 %s 应要求 %s", trigger, role)
		}
	}
	if RequiredRoleForTrigger("UNKNOWN") != "" {
		t.Fatal("未知触发类型不应映射岗位")
	}
}
