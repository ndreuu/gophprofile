# Запуск GophProfile

## Docker Compose

Требуется Docker с Compose. Порты 8080, 5432, 9000, 9001 и 19092 должны быть свободны.

```sh
git clone --branch sprint-11/gophprofile git@github.com:ndreuu/gophprofile.git
cd gophprofile
cp .env.example .env
docker compose up --build -d --wait --wait-timeout 300
```

Первая сборка может занять несколько минут.

- Приложение: http://localhost:8080
- Проверка работоспособности: http://localhost:8080/health
- Консоль MinIO: http://localhost:9001 — логин и пароль заданы в `.env`.

Просмотр логов:

```sh
docker compose logs -f server worker
```

Остановка с сохранением данных:

```sh
docker compose down
```

## Локальный запуск Go-приложений

Требуются Go 1.26+ и Docker с Compose. Команды выполняются из корня проекта.

```sh
cp .env.example .env
docker compose up -d postgres kafka minio
set -a
. ./.env
set +a
go run ./cmd/server
```

Во втором терминале из корня проекта:

```sh
set -a
. ./.env
set +a
go run ./cmd/worker
```

Откройте http://localhost:8080. Для остановки server и worker нажмите `Ctrl+C` в каждом терминале, затем выполните `docker compose down`.

## Демо без Docker

Требуется Go 1.26+. Из корня проекта:

```sh
APP_MODE=demo go run ./cmd/server
```

Откройте http://localhost:8080. Для остановки нажмите `Ctrl+C`. Данные демо хранятся в памяти и сбрасываются после остановки.
