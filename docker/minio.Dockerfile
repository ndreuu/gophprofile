FROM golang:1.27.2-alpine AS builder
RUN apk add --no-cache git
WORKDIR /src
RUN git clone --depth 1 --branch RELEASE.2025-09-07T16-13-09Z https://github.com/minio/minio.git . \
 && test "$(git rev-parse HEAD)" = "07c3a429bfed433e49018cb0f78a52145d4bedeb"
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/minio .

FROM alpine:3.24
RUN apk add --no-cache ca-certificates tzdata \
 && addgroup -S minio && adduser -S -G minio minio \
 && mkdir /data && chown minio:minio /data
COPY --from=builder /out/minio /usr/local/bin/minio
COPY --from=builder /src/LICENSE /usr/share/licenses/minio/LICENSE
USER minio:minio
EXPOSE 9000 9001
ENTRYPOINT ["/usr/local/bin/minio"]
