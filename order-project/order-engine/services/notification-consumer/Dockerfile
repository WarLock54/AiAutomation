# syntax=docker/dockerfile:1
FROM golang:1.23-alpine AS builder
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/notification-consumer ./services/notification-consumer

FROM alpine:3.20
RUN apk add --no-cache ca-certificates && \
    addgroup -S app && adduser -S -G app app
COPY --from=builder /out/notification-consumer /usr/local/bin/notification-consumer

# Container'ı root olarak çalıştırmak, bir process compromise durumunda
# saldırganın container içinde root yetkisi kazanmasına izin verir (bkz.
# rapor [O5]). Non-root bir kullanıcıya geçiyoruz.
USER app

ENTRYPOINT ["/usr/local/bin/notification-consumer"]