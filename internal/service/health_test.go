package service

import "testing"

func TestHealthPayload(t *testing.T) {
	if HealthPayload()["status"] != "ok" {
		t.Fatal("健康状态不符合约定")
	}
}
