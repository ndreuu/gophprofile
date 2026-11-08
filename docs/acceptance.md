# Сверка с ТЗ спринта 11

Проверено 8 ноября 2026. Исходное ТЗ сохранено в `docs/assignment.md`.

| Требование | Реализация / проверка |
|---|---|
| Go 1.21+, Echo/Chi | Go 1.26+, Chi; сборка macOS и Linux arm64 проходит |
| Все перечисленные REST и web маршруты | Реализованы; HTTP unit-тесты и smoke |
| Multipart file, X-User-ID, лимит 10MB | 10 485 760 байт плюс отдельный allowance заголовков multipart; точная граница проверена |
| JPEG, PNG, WebP и MIME/magic bytes | Реальный decode ограниченного изображения; поддельные/повреждённые файлы отклоняются |
| Миниатюры 100×100 и 300×300 | Worker, центрированный квадратный crop, JPEG |
| Получение оригиналов и метаданных | Корректный Content-Type, размеры, имя, время, ссылки на миниатюры |
| Получение аватара пользователя и списка | Последний активный; список новых первыми |
| Параметры size и format | original/100x100/300x300, конвертация jpeg/png/webp без CGO; ошибки формата 400 |
| Кеширование | ETag с учётом размера/формата, Last-Modified, условный GET/304 |
| Удаление | Только владелец; soft delete в БД, асинхронное физическое удаление оригинала и миниатюр |
| Веб-интерфейс | Загрузка, drag/drop, превью, галерея, polling и удаление; адаптивная версия |
| PostgreSQL, миграции и индексы | Embedded migrations, сериализация миграций, метаданные и частичные индексы |
| S3 / MinIO | Приватный бакет; реальные интеграционные проверки MinIO с регионом eu-west-1 прошли |
| Kafka и отдельный worker | Топики, ключ avatar_id, ручные коммиты, отдельная точка входа |
| Идемпотентность | Уникальный message_id, inbox + статус в одной транзакции, детерминированные S3 keys |
| Retry с exponential backoff | Повторы временных ошибок; некорректные события направляются в DLQ |
| Защита от upload/delete гонки | Блокировка строки, проверка удалённого статуса, очистка частичных миниатюр |
| Надёжная публикация | Метаданные и outbox атомарны; событие подтверждается только после публикации |
| /health | БД, S3, брокер; 200/503 и JSON по компонентам |
| Unit-тесты >50% | 66,1% по всему проекту; HTTP, service, repository и изображения покрыты |
| Линтер и ошибки | golangci-lint: 0 issues; race: PASS; внутренние ошибки не раскрываются в JSON |
| Clean Architecture | Domain ports, use cases, transport и инфраструктурные adapters разделены |
| Multi-stage Dockerfile | Сборка Go, минимальный runtime, non-root, embedded web/SQL |
| Docker Compose | server, worker, PostgreSQL, Kafka, MinIO; конфигурация проверена CLI |
| Бонус: rate limiting | По IP, ограниченная память, 429 + Retry-After; health/static не ограничены |
| Бонус: CORS | Явный allowlist из env, preflight, same-origin по умолчанию; wildcard запрещён |
| Комментарии | В Go только документация публичных методов и директивы; обычные JS/CSS/HTML/Python/Docker комментарии удалены |

## Что нельзя считать проверенным локально

Docker Engine/Desktop отсутствует. Поэтому фактические `docker compose build/up` и полный smoke с одновременно работающими PostgreSQL + Apache Kafka + MinIO не выполнялись. Их проверяет подготовленный CI на машине с Docker.

Попытка запустить временный нативный PostgreSQL остановлена sandbox ОС на создании shared memory. SQL unit-тесты проходят, а отдельный тест реального PostgreSQL включён в CI через `POSTGRES_INTEGRATION_URL`; локально он пропущен. Kafka проверена через официальный wire-protocol broker kfake; MinIO проверен настоящим сервером.

## Границы текущего спринта

Kubernetes, Prometheus/Grafana, Loki/ELK, Jaeger и email lookup со стандартной заглушкой относятся к общему плану сервиса. API текущего ТЗ использует user_id; эти будущие возможности не добавлялись в MVP.

## Проверка комментариев

```sh
go run scripts/check_comments.go
```

Проверка анализирует Go AST и допускает doc comments только у экспортируемых методов и методов интерфейсов. `//go:embed` и `//go:build` сохранены: они влияют на сборку. В `go.mod` сохранены генерируемые Go маркеры `// indirect`, а в Python — shebang. Attribution шаблона находится в отдельном LICENSE-файле.
