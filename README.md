# Kubernetes Events Exporter

Самостоятельный exporter Kubernetes Events → VictoriaLogs на Go 1.25+. Использует
один core/v1 Events informer на namespace либо весь кластер, ограниченную очередь,
фиксированное число workers, HTTP retries и согласованную с logs-ui схему полей.
Версия приложения: `0.1.0`; client-go `v0.34.1`.

## Запуск

```sh
export EVENT_EXPORTER_CLUSTER=demo
export EVENT_EXPORTER_ENVIRONMENT=dev
export EVENT_EXPORTER_INGEST=k8s-demo
export EVENT_EXPORTER_SINK_URL='http://localhost:9428/insert/jsonline?_stream_fields=record_type,cluster,kubernetes.namespace_name'
export EVENT_EXPORTER_NAMESPACES=demo-application
export EVENT_EXPORTER_BUSINESS_MAPPINGS='{"demo-application":"demo"}'
export KUBECONFIG="$HOME/.kube/config"
go run ./cmd/event-exporter
```

В Kubernetes используется `rest.InClusterConfig()`. Вне кластера — стандартные
правила clientcmd: список файлов `KUBECONFIG` или `~/.kube/config`. RBAC приложения
разрешает только `get`, `list`, `watch` ресурса `events` в core API group. Запросов
к namespaces, pods, deployments, secrets и discovery для enrichment нет.

## Конфигурация

Все переменные, кроме стандартного `KUBECONFIG`, имеют префикс
`EVENT_EXPORTER_`. Пароли сохраняются без trim; конфигурация и HTTP credentials
не выводятся в логи. URL не должен содержать userinfo или fragment.

| Суффикс | По умолчанию | Назначение |
|---|---|---|
| `CLUSTER`, `ENVIRONMENT`, `INGEST` | обязательны | Кластер, окружение, существующий источник logs-ui |
| `SINK_URL` | обязателен | Полный HTTP(S) URL `/insert/jsonline` с необходимыми параметрами |
| `SINK_USERNAME`, `SINK_PASSWORD` | пусто | Basic auth из Secret; TLS certificate verification включена |
| `NAMESPACES` | пусто | Список через запятую; пусто означает cluster-wide |
| `BUSINESS_MAPPINGS` | `{}` | JSON map точного namespace → business |
| `BUSINESS_PREFIX_MAPPINGS` | `{}` | JSON map явного namespace prefix → business |
| `SERVICE_MAPPINGS` | `{}` | JSON map `namespace/Kind/object-name` → service |
| `HTTP_ADDRESS` | `:2112` | Метрики и probes |
| `QUEUE_CAPACITY` | `1024` | Максимум ожидающих snapshot |
| `WORKERS` | `2` | Параллельные exports, 1–128 |
| `DEDUP_CAPACITY` | `16384` | Максимум успешно отправленных ID в LRU |
| `MAX_RETRIES` | `5` | Дополнительные попытки, 0–20 |
| `RETRY_INITIAL_DELAY` | `200ms` | Начальный backoff |
| `RETRY_MAX_DELAY` | `10s` | Верхняя граница backoff |
| `EXPORT_TIMEOUT` | `10s` | Deadline одной HTTP-попытки |
| `SHUTDOWN_TIMEOUT` | `30s` | Бюджет завершения workers и HTTP |
| `MAX_EVENT_AGE` | `0s` | Не фильтровать по возрасту; положительное значение исключает старые snapshot |
| `KUBE_QPS`, `KUBE_BURST` | `10`, `20` | API throttling client-go |
| `LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARN`, `ERROR` |

Точное business mapping приоритетнее prefix; среди prefix выбирается самый длинный.
Неизвестный namespace остаётся без `business`. Prefix должен задаваться явно и
учитывать границы имён: `b2cdev01-` безопаснее широкого `b2c` в общем кластере.
Service mapping точный; только для объекта `Service` его имя используется напрямую.
Для Pod имя сохраняется, но service по suffix имени Pod не угадывается.
Поддержка Pod enrichment намеренно отсутствует: ей потребуются отдельные права.

## Схема и доставка

Запись — плоский JSON; dotted keys сохраняют совместимость с logs-ui:

```json
{"_time":"2026-11-17T12:00:00Z","_msg":"Back-off restarting failed container","record_type":"kubernetes_event","ingest":"k8s-demo","cluster":"demo","environment":"dev","business":"demo","kubernetes.namespace_name":"demo-application","kubernetes.pod_name":"checkout-abc","service.name":"checkout","service.source":"configured_mapping","event.uid":"event-uid","event.resource_version":"123","event.id":"sha256-hex","event.type":"Warning","event.reason":"BackOff","event.action":"","event.count":4,"event.object.kind":"Pod","event.object.name":"checkout-abc","event.object.uid":"pod-uid","event.object.namespace":"demo-application","event.first_timestamp":"2026-11-17T11:59:00Z","event.last_timestamp":"2026-11-17T12:00:00Z","event.reporting_controller":"","event.reporting_instance":"","event.source.component":"kubelet"}
```

`_time`: series.lastObservedTime → lastTimestamp → firstTimestamp/eventTime →
creationTimestamp → время приёма. `event.count` — накопительный счётчик, максимум
legacy count и series.count, минимум 1. **Не суммировать count всех snapshot**.
Для Node reference заполняется `kubernetes.node_name`; source.host хранится отдельно,
поскольку не каждый producer использует его как имя Kubernetes Node.

List/Watch, resource versions, relist и reconnection выполняет client-go.
Обрабатываются Add и Update, Delete не экспортируется. Обновление с тем же RV
игнорируется. `event.id` — SHA-256 от cluster/namespace/UID/RV; ID стабилен при retry,
а разные ревизии Event образуют отдельные snapshot.

Dedup LRU содержит **только успешно доставленные ID**; параллельные одинаковые
запросы объединяются на время обработки. Кэш не сохраняется между рестартами.
Повторяются network failures, HTTP 429 и 5xx; backoff экспоненциальный с jitter
50–100% от ограниченного интервала. Остальные HTTP статусы завершают обработку.
Redirect не выполняется, тело ошибки не логируется. После исчерпания retries
snapshot теряется с метрикой и JSON error log.

Handler informer не блокируется на storage. При полной очереди **новый snapshot
отбрасывается** с `queue_drops_total` и warning. Это ограничивает память при отказе
storage. Переполнение не обещает последующего восстановления отброшенной ревизии.
Informer cache также хранит текущие Event objects; его размер зависит от количества
живых Events в выбранных namespaces. Размер отдельного Event ограничивается API.

Kubernetes Events — best-effort источник с ограниченной историей. Informer может
объединять промежуточные обновления; исторические Events до начала API retention
восстановить нельзя. Неоднозначный HTTP timeout после приёма VictoriaLogs способен
создать повторную запись. Нет exactly-once, персистентной очереди и гарантии полного
журнала. Стабильный ID позволяет распознавать повторы в потребителе.

## Эксплуатация

- `/healthz` — процесс обслуживает HTTP; отказ storage не вызывает liveness restart.
- `/readyz` — все informer caches первоначально синхронизированы, workers работают,
  последняя завершённая попытка export успешна и shutdown не начат. До первого
  export storage считается доступным; это не активная проверка sink. Восстановление
  после export error определяется следующим успешным export. Временный disconnect
  Watch не сбрасывает факт начальной синхронизации; ошибки видны в метриках/logs.
- `/metrics` — Prometheus без event/object/UID/message labels.

Метрики с префиксом `kubernetes_event_exporter_`: `processed_total`,
`processing_failures_total`, `queue_drops_total`, `shutdown_drops_total`,
`retries_total`, `duplicates_total`, `filtered_total`,
`kubernetes_watch_errors_total` (включая ошибки List reflector),
`processing_duration_seconds`, `queue_depth`. Duration включает retries.
Processed означает успешную доставку, failures — окончательный отказ snapshot.

JSON `log/slog` пишет в stdout с `level`, `service`; сообщения событий содержат
`namespace`, `event_reason`, ошибки — `error`. При SIGTERM/SIGINT readiness снимается,
informers останавливаются, очередь drain-ится до timeout. По deadline контекст sink
отменяется, остаток очереди учитывается как shutdown drops, workers завершаются,
HTTP закрывается, idle connections освобождаются. Sink interface требует корректно
обрабатывать context cancellation.

[deploy/example.yaml](deploy/example.yaml) содержит Deployment, ServiceAccount,
events-only RBAC, Service, security context и probes. Перед применением заменить
пример image/URL и создать `event-exporter-ingest` Secret внешним менеджером секретов.
Образ из примера ещё нужно собрать и опубликовать. Для namespace scope использовать
Role/RoleBinding в каждом выбранном namespace вместо ClusterRoleBinding и задать
`NAMESPACES`; discovery namespaces не нужен. Только одна replica и `Recreate`: HA
leader election не реализован. Пример scrape: [deploy/prometheus.yaml](deploy/prometheus.yaml).

## Проверки

```sh
make check                  # race, coverage >=90% всего Go-кода, vet, build
make fuzz                   # отдельный 10s fuzz campaign
go test -tags=e2e ./...      # real cluster test пропускается без явных E2E variables
```

Table-driven unit tests проверяют env validation, mappings, timestamps/count/series,
queue overflow, dedup, retry classification/backoff и probes. In-process e2e
`TestE2EFakeKubernetesToHTTP` использует fake client-go и настоящий HTTP sink: Add,
Update, namespaces, временный 503, series, readiness и shutdown. Отдельный lifecycle
тест проверяет reconnect и ошибку List. Это не live Kubernetes/storage проверка.

Настоящий e2e через disposable kind + VictoriaLogs:

```sh
export EVENT_EXPORTER_E2E_VICTORIALOGS_IMAGE='<pinned VictoriaLogs Docker image>'
bash scripts/e2e-kind.sh
```

Нужны запущенный Docker daemon, kind, доступ к образам и Go dependencies. Скрипт
создаёт отдельный кластер, контейнер storage и временный kubeconfig; cleanup удаляет
только созданные ресурсы. `TestRealClusterE2E` запускает exporter в процессе теста
с **реальным** Kubernetes client и проверяет сохранение Event/count update через
VictoriaLogs query API. Для собственного disposable стенда явно задать
`EVENT_EXPORTER_E2E_KUBECONFIG`, `EVENT_EXPORTER_E2E_SINK_URL`,
`EVENT_EXPORTER_E2E_QUERY_URL`; тест создаёт и удаляет свой namespace, поэтому его
учётной записи дополнительно нужны права создания/удаления namespaces и Events.
Exporter продолжает использовать только чтение events. Автоматически текущий
production kubeconfig не выбирается. Результаты локальных проверок: [docs/validation.md](docs/validation.md).

## Источники

Архитектура исследована на основе [mustafaakin/resmoio](https://github.com/mustafaakin/kubernetes-event-exporter)
и [Opsgenie exporter](https://github.com/opsgenie/kubernetes-event-exporter).
Реализация новая, без legacy sink dependencies; Apache-2.0 сохранена, см. [NOTICE](NOTICE).
Пример пользовательского сценария: [Loki/Grafana статья](https://medium.com/@lysetskyyv/collecting-and-visualizing-kubernetes-events-with-loki-and-kubernetes-event-exporter-8f764b78185c).
API: [Kubernetes Events](https://kubernetes.io/docs/reference/kubernetes-api/core/event-v1/),
[VictoriaLogs JSONLine](https://docs.victoriametrics.com/victorialogs/data-ingestion/).
