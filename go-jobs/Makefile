.PHONY: build run-admin run-executor test lint clean docker-up docker-down migrate

# ── Build ─────────────────────────────────────────────────────────────────────
build:
	@echo ">> Building admin..."
	CGO_ENABLED=0 go build -ldflags="-s -w" -o bin/admin ./cmd/admin
	@echo ">> Building executor..."
	CGO_ENABLED=0 go build -ldflags="-s -w" -o bin/executor ./cmd/executor

# ── Run ───────────────────────────────────────────────────────────────────────
run-admin:
	go run ./cmd/admin -config config/config.yaml

run-executor:
	go run ./cmd/executor -config config/executor.dev.yaml

# ── Test ──────────────────────────────────────────────────────────────────────
test:
	go test ./... -v -count=1 -timeout 60s

test-cover:
	go test ./... -coverprofile=coverage.out
	go tool cover -html=coverage.out -o coverage.html

# ── Lint ──────────────────────────────────────────────────────────────────────
lint:
	golangci-lint run ./...

# ── Database ──────────────────────────────────────────────────────────────────
migrate:
	mysql -u root -ppassword go_jobs < migration/V1__init_schema.sql
	mysql -u root -ppassword go_jobs < migration/V2__alarm_and_stats.sql
	mysql -u root -ppassword go_jobs < migration/V3__child_job_and_metrics.sql
	@echo ">> Migration complete"

# ── Docker ────────────────────────────────────────────────────────────────────
docker-up:
	docker compose -f deploy/docker/docker-compose.yml up -d

docker-down:
	docker compose -f deploy/docker/docker-compose.yml down

docker-build:
	docker compose -f deploy/docker/docker-compose.yml build

# ── Clean ─────────────────────────────────────────────────────────────────────
clean:
	rm -rf bin/ logs/ coverage.out coverage.html
