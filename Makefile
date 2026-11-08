.PHONY: test coverage lint build demo up down smoke check comments

test:
	go test -race ./...

coverage:
	go test -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out
	@go tool cover -func=coverage.out | awk '/^total:/ {gsub(/%/, "", $$3); if ($$3 <= 50) exit 1}'

comments:
	go run scripts/check_comments.go

lint: comments
	golangci-lint run

build:
	mkdir -p bin
	go build -o bin/server ./cmd/server
	go build -o bin/worker ./cmd/worker

demo:
	APP_MODE=demo go run ./cmd/server

up:
	docker compose up --build -d --wait

down:
	docker compose down

smoke:
	python3 scripts/smoke.py

check: test coverage lint build
