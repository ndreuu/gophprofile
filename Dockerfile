FROM golang:1.27.2-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/server ./cmd/server \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/worker ./cmd/worker

FROM alpine:3.24 AS runtime
RUN apk add --no-cache ca-certificates tzdata \
 && addgroup -S app && adduser -S -G app app
WORKDIR /app
COPY --from=builder /out/server /out/worker ./
USER app:app
EXPOSE 8080
ENTRYPOINT ["/app/server"]
