.PHONY: test coverage lint build demo up down smoke check comments observability-check observability-smoke

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

observability-check:
	docker compose config --quiet
	docker compose run --rm --no-deps --entrypoint /bin/promtool prometheus check config /etc/prometheus/prometheus.yml
	docker compose run --rm --no-deps --entrypoint /bin/promtool prometheus test rules /etc/prometheus/alerts.test.yml
	docker compose run --rm --no-deps --entrypoint /bin/amtool alertmanager check-config /etc/alertmanager/alertmanager.yml
	docker compose run --rm --no-deps loki -config.file=/etc/loki/config.yaml -verify-config=true
	docker compose run --rm --no-deps alloy validate /etc/alloy/config.alloy
	docker compose run --rm --no-deps jaeger validate --config=/etc/jaeger/config.yaml

observability-smoke:
	python3 scripts/observability_smoke.py

check: test coverage lint build
