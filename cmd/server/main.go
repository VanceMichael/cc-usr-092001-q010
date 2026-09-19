package main

import (
	"encoding/json"
	"net/http"
	"os"

	"example.com/batch-092001-q010/internal/service"
)

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Type", "application/json; charset=utf-8")
		_ = json.NewEncoder(response).Encode(service.HealthPayload())
	})
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	if err := http.ListenAndServe("0.0.0.0:"+port, mux); err != nil {
		panic(err)
	}
}
