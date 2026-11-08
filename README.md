# GophProfile

Микросервис аватарок для спринта 11: Go + Chi, PostgreSQL, Kafka, MinIO/S3 и отдельный worker. HTTP API и веб-интерфейс загружают, показывают и удаляют изображения. Worker создаёт миниатюры 100×100 и 300×300 и удаляет файлы в фоне.

## Быстрый запуск полного окружения

Требуется Docker с Compose v2. Порты 8080, 5432, 9000, 9001 и 19092 должны быть свободны.

```sh
cd /Users/andrew/Projects/gophprofile
cp .env.example .env
docker compose up --build -d --wait --wait-timeout 300
```

Откройте http://localhost:8080. Введите ID пользователя, выберите JPEG, PNG или WebP до 10 МиБ, загрузите файл. Галерея обновляет статус обработки автоматически. На карточке доступны оригинал, обе миниатюры и удаление.

- Приложение: http://localhost:8080
- Health: http://localhost:8080/health
- Консоль MinIO: http://localhost:9001 — локальные учётные данные заданы в `.env`.
- PostgreSQL: `localhost:5432`, база/пользователь `gophprofile`.
- Kafka с хоста: `localhost:19092`; внутри Compose: `kafka:9092`.

При старте приложение применяет embedded SQL-миграции, создаёт приватный бакет и топики `avatar-events` и `avatar-events.dlq`. Server и worker безопасно запускают миграции параллельно благодаря advisory lock.

MinIO собирается из официального исходного кода, закреплённого на release и commit в `docker/minio.Dockerfile`: старый готовый образ больше недоступен. Первая сборка потребует времени на зависимости. Лицензия MinIO AGPL включена в его образ.

```sh
python3 scripts/smoke.py          # полный сценарий через HTTP
# или make smoke

docker compose logs -f server worker
docker compose down             # остановить, сохранить данные в volumes
```

## Демо без Docker

Требуется Go 1.26 или новее.

```sh
APP_MODE=demo go run ./cmd/server
```

Откройте http://localhost:8080. Демо использует хранилища и очередь в памяти, запускает worker внутри server и явно помечено в интерфейсе и HTTP-заголовке `X-App-Mode: demo`. Данные исчезают после остановки. Рабочая конфигурация с Kafka/PostgreSQL/S3 запускается через Compose выше.

## Локальная разработка с настоящими зависимостями

```sh
docker compose up -d postgres kafka minio
set -a
. ./.env
set +a
go run ./cmd/server
# Во втором терминале загрузите те же переменные и запустите:
go run ./cmd/worker
```

Файл `.env` не загружается автоматически Go-программой. Compose читает его для подстановки настроек; для `go run` используйте команды выше. Server и worker корректно останавливаются по SIGINT/SIGTERM.

## REST API

В теле ошибок всегда JSON с `error`. Загрузка и удаление требуют `X-User-ID`. По условию проекта этот заголовок является идентификатором пользователя; полноценная аутентификация в MVP отсутствует. Для внешнего доступа его должен устанавливать доверенный auth gateway.

| Метод и маршрут | Поведение |
|---|---|
| `POST /api/v1/avatars` | Multipart, ровно одно поле `file`; ответ 201 со статусом `processing` |
| `GET /api/v1/avatars/{id}` | Бинарный оригинал или миниатюра с `?size=100x100` / `300x300`, `format=jpeg/png/webp` |
| `GET /api/v1/avatars/{id}/metadata` | Метаданные, размеры, статус и ссылки на миниатюры |
| `GET /api/v1/users/{user_id}/avatars` | JSON-массив активных аватарок, новые первыми |
| `GET /api/v1/users/{user_id}/avatar` | Последняя активная аватарка; поддерживает `size` |
| `DELETE /api/v1/avatars/{id}` | Удаление своей аватарки, 204 |
| `DELETE /api/v1/users/{user_id}/avatar` | Удаление последней активной аватарки пользователя |
| `GET /health` | Проверка PostgreSQL, S3 и Kafka; 200 или 503 |
| `GET /web/upload` | Веб-интерфейс загрузки |
| `POST /web/upload` | Загрузка с тем же контрактом и JSON-ответом, что у API |
| `GET /web/gallery/{user_id}` | Галерея пользователя |

Примеры:

```sh
curl -H 'X-User-ID: alice' -F 'file=@photo.png' http://localhost:8080/api/v1/avatars
curl http://localhost:8080/api/v1/users/alice/avatars
curl 'http://localhost:8080/api/v1/avatars/AVATAR_UUID?size=100x100' -o thumbnail.jpg
curl -X DELETE -H 'X-User-ID: alice' http://localhost:8080/api/v1/avatars/AVATAR_UUID
```

Выбранные правила для неоднозначных мест ТЗ:

- Текущая аватарка — самая новая неудалённая; порядок `created_at DESC, id DESC`.
- Во время `processing` оригинал доступен, миниатюры возвращают 409 и `Retry-After: 2`.
- Повторный DELETE своей уже удалённой аватарки по ID возвращает 204. Неизвестный ID — 404, чужая аватарка — 403.
- Миниатюры — квадратный центральный crop в JPEG; прозрачность композится на белый фон. Оригинал сохраняется без изменений. Параметр `format` конвертирует выдачу в JPEG, PNG или WebP; сохранённые файлы не меняются.
- Лимит файла — 10 485 760 байт; дополнительно ограничены размеры изображения и число пикселей до декодирования.
- Поиск по email из общего описания будущего сервиса в текущем контракте не задан: маршруты используют `user_id`.

## Надёжность фоновой обработки

1. Оригинал записывается в S3. Метаданные и событие записываются в одной транзакции PostgreSQL — transactional outbox.
2. Dispatcher в worker публикует событие в Kafka с ключом `avatar_id`. Outbox помечается отправленным только после подтверждения брокера. При сбое задача остаётся в БД.
3. Consumer читает сообщения с ручным коммитом оффсетов. Временные ошибки повторяются с экспоненциальной задержкой; оффсет фиксируется после успешной обработки.
4. Inbox `processed_messages` с уникальным `message_id` и обновление метаданных коммитятся вместе. Повторы не запускают уже завершённую работу заново.
5. Файлы миниатюр имеют детерминированные ключи. Повтор после частичного сбоя перезаписывает те же объекты.
6. Обработка и удаление сериализуются блокировкой строки аватарки. Запоздалое событие загрузки пропускается для удалённой записи; удаление очищает также ключи потенциально частично созданных миниатюр.
7. Невалидные события направляются в DLQ; временные ошибки инфраструктуры повторяются без пропуска сообщений.

Это доставка at-least-once с идемпотентными эффектами в БД/S3. Kafka-транзакции сами по себе не делают запись в PostgreSQL и S3 атомарной. При неопределённом результате коммита оригинал сохраняется, чтобы не удалить успешно записанный файл.

## Структура и границы

```text
cmd/server, cmd/worker          точки входа
internal/domain                модели, ошибки, порты
internal/service               пользовательские сценарии
internal/worker                фоновые сценарии и outbox dispatcher
internal/transport/httpapi     HTTP, JSON, multipart
internal/adapters/postgres     метаданные, inbox/outbox, транзакции
internal/adapters/s3           приватный бакет и объектные операции
internal/adapters/kafka        публикация, consumer, DLQ
internal/adapters/memory       явный режим demo
internal/imageproc             валидация, resize/crop
internal/config, internal/app  конфигурация и wiring
migrations                     embedded SQL
web                            embedded HTML/CSS/JS
```

Domain и service не зависят от PostgreSQL, Kafka, MinIO или HTTP. Веб-интерфейс адаптирован из MIT-шаблона Yandex Practicum, attribution сохранён в `web/LICENSE.yandex-practicum`.

## Проверки

```sh
go test -race ./...
make coverage                  # завершится ошибкой при покрытии <= 50%
go run scripts/check_comments.go
go vet ./...
golangci-lint run               # golangci-lint v2.15.0
make build
python3 scripts/smoke.py        # требуется запущенный server и worker
```

Дополнительные тесты адаптера Kafka используют официальный protocol-test broker kfake и запускаются отдельно:

```sh
go test -race -tags=integration ./internal/adapters/kafka
```

CI выполняет unit/race, порог покрытия, линтер, сборку, проверку Compose и smoke полного окружения.

## Сдача

Создайте свой репозиторий, отправьте код в отдельную ветку и откройте Pull Request. В Практикуме нажмите «Сдать работу» и укажите ссылку на PR. После исправлений отправляйте ссылку повторно через платформу.

## CORS и лимиты запросов

По умолчанию браузерные изменения разрешены с origin самого приложения. Дополнительные origins задаются через `CORS_ALLOWED_ORIGINS=https://frontend.example,http://localhost:3000`; wildcard и URL с путями запрещены. Лимит API — `RATE_LIMIT_REQUESTS=120` за `RATE_LIMIT_WINDOW=1m` на IP. Ответ 429 содержит Retry-After. Заголовок X-Forwarded-For не используется для определения IP; health и статика не ограничены.

Обычные комментарии в коде удалены. CI запускает AST-проверку, допускающую документацию публичных методов и необходимые директивы Go.

Интеграционные проверки реального S3 и PostgreSQL:

```sh
S3_INTEGRATION_ENDPOINT=localhost:9000 go test -race -tags=integration ./internal/adapters/s3
POSTGRES_INTEGRATION_URL='postgres://gophprofile:gophprofile_dev@localhost:5432/gophprofile?sslmode=disable' go test -race -tags=integration ./internal/adapters/postgres
```
