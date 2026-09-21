package main

import (
	"log"
	"net/http"
	"os"

	"example.com/batch-092001-q010/internal/engine"
	"example.com/batch-092001-q010/internal/service"
	"example.com/batch-092001-q010/internal/store"
)

func main() {
	// DATABASE_PATH 指向仅追加事件日志文件（JSONL）。未配置时退化为内存日志，
	// 仅适用于本地演示——重启后事实会丢失，应急部署必须显式配置持久化路径。
	var eventLog store.Log
	if path := os.Getenv("DATABASE_PATH"); path != "" {
		fileLog, err := store.OpenFileLog(path)
		if err != nil {
			log.Fatalf("打开事件日志失败: %v", err)
		}
		log.Printf("事件日志已持久化到 %s（已载入 %d 条事实）", path, fileLog.Len())
		eventLog = fileLog
	} else {
		log.Println("未设置 DATABASE_PATH，使用内存事件日志（重启后事实丢失，请勿用于应急部署）")
		eventLog = store.NewMemoryLog()
	}
	defer eventLog.Close()

	controller, err := engine.New(eventLog)
	if err != nil {
		log.Fatalf("初始化任务控制器失败: %v", err)
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	server := &http.Server{
		Addr:    "0.0.0.0:" + port,
		Handler: service.NewServer(controller).Handler(),
	}
	log.Printf("灾区空中通信任务控制器监听 :%s", port)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
