# Проверка реализации

Локальная проверка: 8 ноября 2026, macOS arm64, Go 1.27.2.

| Проверка | Результат |
|---|---|
| `go test -race -coverprofile=coverage.out ./...` | PASS, общее покрытие 66,1% |
| `go test -race -tags=integration ./internal/adapters/kafka -count=1` | PASS, 5 интеграционных сценариев через kfake |
| `golangci-lint run` (v2.15.0) | 0 issues |
| Сборка `cmd/server` и `cmd/worker` для macOS arm64 | PASS |
| `CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build ./cmd/...` | PASS |
| Сборка закреплённого MinIO из исходников для Linux arm64 | PASS |
| Compose v5.7.0 `-f compose.yaml config -q` | PASS |
| `python3 scripts/smoke.py` на режиме demo | PASS |
| Проверка в браузере | Загрузка, polling, обе ссылки на миниатюры, открытие 100×100, удаление, повторная загрузка — PASS |
| Мобильный экран 390×844 | Нет горизонтального переполнения |
| Console warn/error | Пусто |

Покрытие отдельных пакетов: domain 95,0%, service 90,8%, HTTP 95,2%, imageproc 91,7%, S3 86,7%, PostgreSQL 78,9%, Kafka 77,4%, worker 77,0%, config 98,9%. Общее покрытие включает также точки входа, demo adapters и wiring без отдельных unit-тестов.

Kafka integration проверяет реальный сетевой протокол на официальном тестовом брокере kfake: создание топиков, доставку, retry без пропуска сообщения, сохранение оффсетов после перезапуска consumer, poison event в DLQ и отмену. Это не запуск полноценного Apache Kafka.

S3 unit-тесты проверяют HTTP-контракт, приватный бакет, ошибки и идемпотентное удаление через тестовый Transport. PostgreSQL unit-тесты используют pgxmock и проверяют транзакции, блокировки, inbox/outbox и защиту владельца.

**Ограничение окружения:** Docker Engine/Desktop не установлен. Фактические `docker compose build/up` и smoke полного окружения с PostgreSQL + MinIO + Apache Kafka локально не выполнялись. Для этой проверки подготовлены Compose, `scripts/smoke.py` и CI. Сборка Go-приложений и исходного MinIO проверена отдельно; YAML проверен официальным Compose CLI без daemon.

Файлы приложения готовы для следующей проверки на машине с Docker:

```sh
docker compose up --build -d --wait --wait-timeout 300
python3 scripts/smoke.py
```

## Дополнительный аудит по полному ТЗ

- Добавлена проверенная конвертация выдачи в JPEG, PNG и WebP, включая корректный размер ответа и разные ETag.
- Добавлены CORS allowlist, preflight, IP rate limiting, валидация конфигурации и регрессионные тесты.
- Исправлены строгая проверка X-User-ID, ошибочный пустой HTTP 200 при сбое чтения S3, автоматический выбор региона S3, обработка oversized poison event.
- `go run scripts/check_comments.go`: PASS. В Go сохранены только doc comments публичных методов/методов интерфейсов и необходимые директивы сборки. Обычные комментарии остальных исходных файлов удалены.
- Реальный MinIO, запущенный с регионом eu-west-1: `TestRealS3StorageLifecycleAndPrivacy` PASS, включая запрет анонимного доступа и повторное удаление.
- PostgreSQL native bootstrap заблокирован sandbox ОС (`shmget: Operation not permitted`); тест реальной БД компилируется и включён в CI, локально пропущен при отсутствии POSTGRES_INTEGRATION_URL.
- Обновлённый smoke: PASS, включая все три формата для оригинала и обеих миниатюр.

Полная матрица: `docs/acceptance.md`.
