# syntax=docker/dockerfile:1
FROM golang:1.23-alpine AS builder
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/payment-service ./services/payment-service

# grpc_health_probe: Docker'ın HEALTHCHECK direktifiyle standart gRPC Health
# Checking Protocol'ü sorgulayabilmesi için küçük bir binary (K8s liveness/
# readiness probe'ları da genelde aynı binary'yi kullanır).
FROM alpine:3.20 AS healthprobe
ARG GRPC_HEALTH_PROBE_VERSION=v0.4.24
# checksums.txt, AYNI release'in resmi checksum listesidir; binary bu listeye
# karşı doğrulanmadan çalıştırılmaz (bkz. rapor [O5] -- eskiden checksum
# doğrulaması hiç yapılmıyordu, indirilen binary olduğu gibi güvenilirdi).
RUN apk add --no-cache wget && \
    cd /tmp && \
    wget -q https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64 && \
    wget -q https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/checksums.txt && \
    grep "grpc_health_probe-linux-amd64$" checksums.txt | sha256sum -c - && \
    mv grpc_health_probe-linux-amd64 /bin/grpc_health_probe && \
    chmod +x /bin/grpc_health_probe

FROM alpine:3.20
RUN apk add --no-cache ca-certificates && \
    addgroup -S app && adduser -S -G app app
COPY --from=builder /out/payment-service /usr/local/bin/payment-service
COPY --from=healthprobe /bin/grpc_health_probe /usr/local/bin/grpc_health_probe

HEALTHCHECK --interval=10s --timeout=5s --start-period=5s --retries=5 \
  CMD ["/usr/local/bin/grpc_health_probe", "-addr=:50053"]

# Container'ı root olarak çalıştırmak, bir process compromise durumunda
# saldırganın container içinde root yetkisi kazanmasına izin verir (bkz.
# rapor [O5]). Non-root bir kullanıcıya geçiyoruz.
USER app

ENTRYPOINT ["/usr/local/bin/payment-service"]