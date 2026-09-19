.PHONY: test migrate run
test:
	go test ./...
migrate:
	sh scripts/migrate.sh
run:
	go run ./cmd/server
