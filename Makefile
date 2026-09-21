.PHONY: test test-race migrate run fmt vet

test:
	go test ./...

test-race:
	go test -race -count=1 ./...

migrate:
	sh scripts/migrate.sh

run:
	go run ./cmd/server

fmt:
	gofmt -l -w .

vet:
	go vet ./...
