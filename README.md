# Запуск GophProfile

## Docker Compose

Требуется Docker с Compose и не менее 4 ГБ RAM для Docker.

```sh
git clone --branch sprint12 git@github.com:ndreuu/gophprofile.git
cd gophprofile
cp .env.example .env
docker compose up --build -d --wait --wait-timeout 300
```

Если проект уже скачан, выполняйте команды из его корня. Существующий `.env` сохраните и добавьте недостающие параметры из `.env.example`.

После запуска:

- Приложение: http://localhost:8080
- Grafana: http://localhost:3000/d/gophprofile-overview — логин и пароль из `GRAFANA_ADMIN_USER` и `GRAFANA_ADMIN_PASSWORD` в `.env`.
- Prometheus: http://localhost:9090
- Jaeger: http://localhost:16686
- MinIO: http://localhost:9001 — логин и пароль из `MINIO_ROOT_USER` и `MINIO_ROOT_PASSWORD` в `.env`.

Логи приложения:

```sh
docker compose logs -f server worker
```

Остановка:

```sh
docker compose down
```

## Демо без Docker

Требуется Go 1.26+. Из корня проекта:

```sh
APP_MODE=demo go run ./cmd/server
```

Откройте http://localhost:8080. Остановка — `Ctrl+C`. Данные демо хранятся в памяти.
