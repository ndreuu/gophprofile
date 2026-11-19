# Запуск GophProfile

## Docker Compose

Требуется Docker с Compose. Для полного локального стенда выделите Docker не менее 4 ГБ RAM: шесть сервисов мониторинга ограничены суммарно примерно 2,1 ГБ, отдельно работают Go, PostgreSQL, MinIO и Kafka. Нужны свободные порты 8080, 5432, 9000, 9001, 19092, 3000, 9090, 9091, 9093, 3100, 4318 и 16686. Все опубликованные порты привязаны к localhost.

```sh
git clone --branch sprint12 git@github.com:ndreuu/gophprofile.git
cd gophprofile
cp .env.example .env
docker compose up --build -d --wait --wait-timeout 300
```

Первая сборка может занять несколько минут.

При обновлении существующего `.env` добавьте новые параметры из `.env.example`, включая `GRAFANA_ADMIN_PASSWORD`.

- Приложение: http://localhost:8080
- Проверка работоспособности: http://localhost:8080/health
- Консоль MinIO: http://localhost:9001 — логин и пароль заданы в `.env`.
- Grafana: http://localhost:3000/d/gophprofile-overview — логин `GRAFANA_ADMIN_USER`, пароль `GRAFANA_ADMIN_PASSWORD` из `.env`. Datasources и dashboard `GophProfile · Service overview` загружаются автоматически.
- Prometheus: http://localhost:9090 — [targets](http://localhost:9090/targets), [alert rules](http://localhost:9090/alerts).
- Метрики server: http://localhost:8080/metrics; worker: http://localhost:9091/metrics, health worker: http://localhost:9091/health.
- Jaeger: http://localhost:16686 — сервисы `gophprofile-server` и `gophprofile-worker`; OTLP/HTTP collector на `localhost:4318`.
- Loki: http://localhost:3100/ready — логи смотрите через Grafana Explore → Loki.
- Alertmanager: http://localhost:9093 — локальные алерты без отправки внешних уведомлений.

Просмотр логов:

```sh
docker compose logs -f server worker
```

Остановка с сохранением данных:

```sh
docker compose down
```

Метрики и логи хранятся в named volumes, retention — 24 часа, для Prometheus также задан предел блоков 512 МБ (WAL/head могут занимать дополнительное место). Jaeger держит до 5000 traces в памяти: после перезапуска его история сбрасывается. Alloy читает stdout/stderr только контейнеров приложения через Docker API, для этого ему смонтирован `/var/run/docker.sock`. Позиции чтения сохраняются; JSON-логи Docker ротируются по 3 файла по 10 МБ на процесс.

## Проверка мониторинга

```sh
make observability-check  # валидаторы конфигов и unit-тесты alert rules
make smoke               # проверка функций приложения
make observability-smoke # upload → worker → metrics + logs + distributed trace
```

Последняя команда создаёт временный PNG, передаёт W3C `traceparent`, ждёт обработки и проверяет Prometheus, provisioning Grafana, один trace с server и worker и логи обоих процессов с тем же `trace_id` в Loki. После проверки аватар удаляется. Команда печатает ID проверенного trace. CI выполняет эти же проверки.

Для ручной проверки загрузите изображение через приложение, дождитесь thumbnails и откройте dashboard. Первый scrape происходит в течение 15 секунд; для `rate` нужны как минимум две точки. Пустые графики uploads/worker до первой операции ожидаемы. Панели показывают:

- HTTP RED: requests/sec по method/route, долю 5xx, статусы, P50/P95/P99 и P95 по маршруту;
- ресурсы server/worker: CPU процесса, RSS и Go heap, goroutines, соединения pgx pool (acquired/idle/total);
- бизнес: uploads по исходу и P95, объём хранения, глубину outbox, исходы и P95 обработки событий worker;
- доступность scrape targets, firing alerts и JSON-логи.

Примеры PromQL в Prometheus или Grafana Explore → Prometheus:

```promql
sum by (method, route) (rate(gophprofile_http_requests_total[5m]))

histogram_quantile(0.95,
  sum by (le, route) (rate(gophprofile_http_request_duration_seconds_bucket[5m])))

sum(rate(gophprofile_http_requests_total{status=~"5.."}[5m]))
  / sum(rate(gophprofile_http_requests_total[5m]))

gophprofile_db_connections{state="acquired"}
gophprofile_storage_bytes
gophprofile_outbox_queue_depth
sum by (event_type, status) (increase(gophprofile_worker_events_total[5m]))
```

`gophprofile_outbox_queue_depth` считает ожидающие публикации события в PostgreSQL outbox, а не Kafka consumer lag. Route — шаблон маршрута; ID аватаров, пользователей, requests и traces не являются labels метрик.

`gophprofile_storage_bytes` — логический объём оригиналов и thumbnails активных аватаров по данным приложения; он не отражает свободное место диска или физический размер MinIO bucket. Размеры thumbnails сохраняются начиная со спринта 12; старые записи без размера учитывают только оригинал. Технические `/health` и `/metrics` исключены из HTTP RED и tracing.

Примеры LogQL в Grafana Explore → Loki:

```logql
{app="gophprofile"} | json
{app="gophprofile", service="worker"} | json
{app="gophprofile"} | json | level="ERROR"
{app="gophprofile"} | json | trace_id="ID_ИЗ_SMOKE"
```

Раскройте JSON-строку и нажмите `trace_id → View trace`: откроется Jaeger datasource с этим trace. Обратная ссылка в span (`Logs for this span`) ищет все логи с этим trace ID в окне ±2 минуты. В Jaeger по одному ID видны HTTP, service/DB/S3 и асинхронная цепочка outbox → Kafka → worker. `trace_id`, `span_id`, `request_id` остаются полями JSON, без индексации как Loki labels.

`OTEL_TRACES_SAMPLER_ARG` задаёт долю новых traces от 0 до 1; для локальной проверки используется `1`. Входящий sampled-флаг W3C учитывается parent-based sampler. В Compose server и worker получают разные `OTEL_SERVICE_NAME`, collector — `http://jaeger:4318`. `LOG_LEVEL` задаёт уровень структурированных slog-логов.

В `observability/prometheus/alerts.yml` настроены 5xx > 5% и P95 > 500 мс за 5 минут при минимум 20 запросах, оба условия должны сохраняться 2 минуты. Недоступный target вызывает critical alert через минуту. Loki ruler также отправляет warning при более 5 ERROR-логах за 5 минут в течение 2 минут (`observability/loki/rules/fake/application.yml`). Проверить переход pending → firing можно в Prometheus Alerts, log rule — через `http://localhost:3100/prometheus/api/v1/rules`; доставка в локальный Alertmanager видна на `:9093`. Проверки порогов, выдержки и малого трафика HTTP выполняет `promtool test rules` в `make observability-check`.

Конфигурация основана на официальных документах [Alloy Docker logs](https://grafana.com/docs/alloy/latest/reference/components/loki/loki.source.docker/), [Loki filesystem](https://grafana.com/docs/loki/latest/configure/examples/configuration-examples/), [Jaeger v2](https://www.jaegertracing.io/docs/2.20/getting-started/) и [Grafana trace correlation](https://grafana.com/docs/grafana/latest/datasources/jaeger/configure/). Версии Docker images закреплены в `compose.yaml`. Jaeger закреплён на `2.20.0`: [в 2.21 удалены legacy HTTP endpoints](https://github.com/jaegertracing/jaeger/releases/tag/v2.21.0), которые [Grafana Jaeger datasource использует для поиска](https://github.com/grafana/grafana-jaeger-datasource/blob/main/pkg/jaeger/client.go). Это сохраняет поиск и открытие traces в Grafana.

## Локальный запуск Go-приложений

Требуются Go 1.26+ и Docker с Compose. Команды выполняются из корня проекта.

```sh
cp .env.example .env
docker compose up -d postgres kafka minio
set -a
. ./.env
set +a
OTEL_SERVICE_NAME=gophprofile-server go run ./cmd/server
```

Во втором терминале из корня проекта:

```sh
set -a
. ./.env
set +a
OTEL_SERVICE_NAME=gophprofile-worker go run ./cmd/worker
```

Откройте http://localhost:8080. Для остановки server и worker нажмите `Ctrl+C` в каждом терминале, затем выполните `docker compose down`.

Для локальных Go-процессов `.env.example` указывает collector `http://localhost:4318`; запустите его командой `docker compose up -d jaeger`. В полном Compose используются внутренние имена `server:8080` и `worker:9091`; для scrape локальных процессов замените targets в `observability/prometheus/prometheus.yml` на доступный из Docker адрес хоста. Alloy в стандартном конфиге собирает только контейнерные логи.

## Демо без Docker

Требуется Go 1.26+. Из корня проекта:

```sh
APP_MODE=demo go run ./cmd/server
```

Откройте http://localhost:8080. Для остановки нажмите `Ctrl+C`. Данные демо хранятся в памяти и сбрасываются после остановки.
