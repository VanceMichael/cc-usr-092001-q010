package service

// HealthPayload 返回运行环境使用的健康状态。
func HealthPayload() map[string]string {
	return map[string]string{"status": "ok"}
}
