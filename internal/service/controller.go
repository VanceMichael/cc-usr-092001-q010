// Package service 提供任务控制器的 HTTP 边界。
//
// 所有写入都是"提交事件信封"，控制器不提供任何直接改状态的接口：
// 断链、重试、补传都以同一事件协议进入，由引擎保证幂等与顺序。
package service

import (
	"encoding/json"
	"errors"
	"net/http"

	"example.com/batch-092001-q010/internal/domain"
	"example.com/batch-092001-q010/internal/engine"
)

// Server 持有控制器引擎并暴露 HTTP 路由。
type Server struct {
	Engine *engine.Engine
}

// NewServer 装配控制器服务。
func NewServer(e *engine.Engine) *Server {
	return &Server{Engine: e}
}

// Handler 返回已注册全部路由的 mux。
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, HealthPayload())
	})

	// 统一事件摄入：外部系统（无人机、基站、岗位终端、离线补传网关）只走这一个入口。
	mux.HandleFunc("POST /v1/events/ingest", s.handleIngest)

	// 事实只读视图。
	mux.HandleFunc("GET /v1/state", s.handleState)
	mux.HandleFunc("GET /v1/events", s.handleEvents)
	mux.HandleFunc("GET /v1/tasks/{taskRef}", s.handleTask)
	mux.HandleFunc("GET /v1/plans/{planRef}", s.handlePlan)
	mux.HandleFunc("GET /v1/retasks", s.handleRetasks)
	mux.HandleFunc("GET /v1/queues", s.handleQueues)

	// 事后还原：输入一次通话或侦察成果，还原当时位置、覆盖、抢占与责任链。
	mux.HandleFunc("GET /v1/reconstruct/call/{callID}", s.handleReconstructCall)
	mux.HandleFunc("GET /v1/reconstruct/recon/{reconID}", s.handleReconstructRecon)

	return mux
}

// handleIngest 接收事件信封。重复 event_id 返回 200 + duplicate=true；
// 序号缺口返回 202 + buffered=true；业务拒绝返回 422 并附原因码。
func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	var env domain.Envelope
	if err := json.NewDecoder(r.Body).Decode(&env); err != nil {
		writeError(w, http.StatusBadRequest, "VALIDATION_FAILED", "事件信封不是合法 JSON: "+err.Error())
		return
	}
	result, err := s.Engine.Ingest(env)
	if err != nil {
		var reject *engine.RejectError
		if errors.As(err, &reject) {
			status := http.StatusUnprocessableEntity
			if reject.Code == engine.RejectStaleSequence || reject.Code == engine.RejectSequenceConflict {
				status = http.StatusConflict
			}
			if reject.Code == engine.RejectValidation {
				status = http.StatusBadRequest
			}
			writeError(w, status, string(reject.Code), reject.Reason)
			return
		}
		writeError(w, http.StatusInternalServerError, "INTERNAL", err.Error())
		return
	}
	status := http.StatusOK
	if result.Buffered {
		status = http.StatusAccepted
	}
	writeJSON(w, status, result)
}

func (s *Server) handleState(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.Engine.Snapshot())
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	limit := 0
	if n := r.URL.Query().Get("limit"); n != "" {
		if v, err := parsePositive(n); err == nil {
			limit = v
		}
	}
	events, err := s.Engine.Events(limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": events, "count": len(events)})
}

func (s *Server) handleTask(w http.ResponseWriter, r *http.Request) {
	taskRef := r.PathValue("taskRef")
	snap := s.Engine.Snapshot()
	task, ok := snap.Tasks[taskRef]
	if !ok {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "任务 "+taskRef+" 尚无事实记录")
		return
	}
	writeJSON(w, http.StatusOK, task)
}

func (s *Server) handlePlan(w http.ResponseWriter, r *http.Request) {
	planRef := r.PathValue("planRef")
	snap := s.Engine.Snapshot()
	plan, ok := snap.Plans[planRef]
	if !ok {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "计划 "+planRef+" 尚未签署")
		return
	}
	writeJSON(w, http.StatusOK, plan)
}

func (s *Server) handleRetasks(w http.ResponseWriter, _ *http.Request) {
	snap := s.Engine.Snapshot()
	writeJSON(w, http.StatusOK, map[string]any{"retasks": snap.Retasks})
}

func (s *Server) handleQueues(w http.ResponseWriter, _ *http.Request) {
	snap := s.Engine.Snapshot()
	writeJSON(w, http.StatusOK, map[string]any{"queues": snap.Queues})
}

func (s *Server) handleReconstructCall(w http.ResponseWriter, r *http.Request) {
	callID := r.PathValue("callID")
	rec, err := s.Engine.ReconstructCall(callID)
	if err != nil {
		var reject *engine.RejectError
		if errors.As(err, &reject) {
			writeError(w, http.StatusNotFound, string(reject.Code), reject.Reason)
			return
		}
		writeError(w, http.StatusInternalServerError, "INTERNAL", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, rec)
}

func (s *Server) handleReconstructRecon(w http.ResponseWriter, r *http.Request) {
	reconID := r.PathValue("reconID")
	rec, err := s.Engine.ReconstructRecon(reconID)
	if err != nil {
		var reject *engine.RejectError
		if errors.As(err, &reject) {
			writeError(w, http.StatusNotFound, string(reject.Code), reject.Reason)
			return
		}
		writeError(w, http.StatusInternalServerError, "INTERNAL", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, rec)
}

// errorBody 是统一错误响应。
type errorBody struct {
	Code   string `json:"code"`
	Reason string `json:"reason"`
}

func writeError(w http.ResponseWriter, status int, code, reason string) {
	writeJSON(w, status, errorBody{Code: code, Reason: reason})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
