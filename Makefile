.PHONY: test race coverage fuzz vet build check

test:
	go test ./...
race:
	go test -race -coverprofile=coverage.out ./...
coverage: race
	go tool cover -func=coverage.out
	@go tool cover -func=coverage.out | awk '/^total:/ { if ($$3+0 < 90) exit 1 }'
fuzz:
	go test ./internal/exporter -run '^$$' -fuzz '^FuzzNormalize$$' -fuzztime=10s
	go test ./internal/exporter -run '^$$' -fuzz '^FuzzLoadConfig$$' -fuzztime=10s
vet:
	go vet ./...
build:
	CGO_ENABLED=0 go build -trimpath -o bin/event-exporter ./cmd/event-exporter
check: coverage vet build
