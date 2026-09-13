.PHONY: proto build up down logs tidy test clean

# ---------------------------------------------------------------------------
# Proto code generation
# ---------------------------------------------------------------------------
# Gereksinimler (yerel makinede kurulu olmalı):
#   protoc                      -> apt install -y protobuf-compiler  (ya da brew install protobuf)
#   protoc-gen-go                -> go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
#   protoc-gen-go-grpc           -> go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
# İkisi de $GOPATH/bin altına kurulur; o dizinin $PATH'te olduğundan emin olun.
proto:
	mkdir -p gen/order/v1 gen/inventory/v1 gen/payment/v1
	protoc \
		--go_out=gen --go_opt=module=order-engine/gen \
		--go-grpc_out=gen --go-grpc_opt=module=order-engine/gen \
		--proto_path=proto \
		proto/order.proto proto/inventory.proto proto/payment.proto
	@echo "Proto kodları gen/ altında üretildi."

# ---------------------------------------------------------------------------
# Build & run
# ---------------------------------------------------------------------------
tidy:
	go mod tidy

build: proto
	go build ./...

up:
	docker compose up --build -d

down:
	docker compose down -v

logs:
	docker compose logs -f order-service inventory-service payment-service notification-consumer

test:
	go test ./... -race -v

clean:
	rm -rf gen
