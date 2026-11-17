# Локальная проверка

Дата: 2026-11-17. Среда: macOS arm64, Go 1.25.3. Проверки не обращались к
рабочим Kubernetes/Vault/registry endpoints. Go dependencies загружены через
разрешённый сетевой доступ; HTTP-тесты требовали localhost listeners вне sandbox.

| Проверка | Результат |
|---|---|
| `go test -race -coverprofile=coverage-race.out ./...` | PASS, data races не обнаружены |
| `go tool cover -func=coverage-race.out` | **93.9% statements всего Go-кода** |
| Package `internal/exporter` | **99.4% statements** |
| Package `cmd/event-exporter` | 0% unit coverage; включён в общий denominator |
| `go vet ./...` | PASS |
| `CGO_ENABLED=0 go build -trimpath -o bin/event-exporter ./cmd/event-exporter` | PASS |
| `FuzzNormalize`, 10s | PASS, 314867 executions |
| `FuzzLoadConfig`, 10s | PASS, 275142 executions |
| `TestE2EFakeKubernetesToHTTP` | PASS: настоящий informer + fake client-go, HTTP 503/retry → JSONLine |
| `TestWatchErrorAndReconnect` | PASS: List error, восстановление Watch и следующая ревизия |
| Shutdown / early controller failure / incomplete HTTP request tests | PASS: отмена, join, pending drops, timeout, закрытие HTTP |
| stdout JSON smoke запуска binary без env | PASS: exit 1 и JSON `level=ERROR`, `service`, `error` |
| `bash -n scripts/e2e-kind.sh` | PASS |
| YAML parse и проверка Deployment/Service/SA/events-only RBAC | PASS |
| `git diff --check` | PASS |
| `go test -tags=e2e ./internal/exporter -run '^TestRealClusterE2E$' -v` | Компилируется; **SKIP**, explicit disposable endpoints не заданы |
| Docker build / kind / live VictoriaLogs e2e | **Не выполнены**: Docker daemon недоступен |

`docker info` и `kind get clusters` проверены: оба отвечают `Cannot connect to
the Docker daemon`. Образ не собран/не опубликован, настоящий Kubernetes/storage
контур не проверен. Для этого подготовлен opt-in `scripts/e2e-kind.sh`, описанный
в README. GitHub remote настроен; push в рамках этой работы не выполняется.

Darwin linker при race-сборке выдаёт warning `malformed LC_DYSYMTAB` для object
из race runtime. Процесс тестов завершается с exit 0; это предупреждение toolchain,
а не обнаруженная гонка. Оставшиеся непокрытые production paths — CLI wiring и
ошибки регистрации handler у только что созданного informer.

Fuzz campaigns проверяют отсутствие panic/нарушений базовой схемы и устойчивую
валидацию конфигурации. Покрытие — statement coverage Go, не branch coverage.
Числа относятся к default build, без opt-in live test. `make coverage` проверяет
порог 90% по всему production Go-коду, включая CLI.
