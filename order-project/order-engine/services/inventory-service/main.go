package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	inventoryv1 "order-engine/gen/inventory/v1"
	"order-engine/pkg/grpcmiddleware"
	"order-engine/pkg/health"
	"order-engine/pkg/idempotency"
	"order-engine/pkg/tracing"
	"order-engine/services/inventory-service/internal"
)

var logger = slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "inventory-service")

func main() {
	grpcPort := mustEnv("GRPC_PORT")
	postgresDSN := mustEnv("POSTGRES_DSN")
	redisAddr := mustEnv("REDIS_ADDR")

	// Distributed Tracing: order-service'ten gelen trace context'i
	// (traceparent) otomatik olarak devralınır -- otelgrpc StatsHandler
	// bunu grpc.NewServer'a eklendiğinde yönetir (aşağıda).
	shutdownTracing, err := tracing.Init(context.Background(), "inventory-service", envOrDefault("OTLP_ENDPOINT", "jaeger:4317"))
	if err != nil {
		logger.Warn("tracing başlatılamadı, izleme olmadan devam ediliyor", "error", err)
	} else {
		defer shutdownTracing(context.Background())
	}

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()

	idemStore := idempotency.NewStore(rdb, "inventory", 24*time.Hour)

	repo, err := internal.NewPostgresRepository(postgresDSN)
	if err != nil {
		logger.Error("postgres bağlantısı kurulamadı", "error", err)
		os.Exit(1)
	}
	defer repo.Close()

	svc := internal.NewInventoryServer(repo, idemStore)

	grpcServer := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.ChainUnaryInterceptor(
			grpcmiddleware.RecoveryInterceptor(),
			grpcmiddleware.LoggingInterceptor(),
		),
	)
	inventoryv1.RegisterInventoryServiceServer(grpcServer, svc)

	// gRPC Health Checking Protocol: K8s liveness/readiness probe'ları
	// bu servisi sorgulayabilir (bkz. Proje 3).
	healthReporter := health.Register(grpcServer, "inventory.v1.InventoryService")

	// grpcurl gibi araçların .proto dosyası vermeden servisleri
	// keşfedebilmesi (list/describe/health check) için reflection açık.
	reflection.Register(grpcServer)

	lis, err := net.Listen("tcp", ":"+grpcPort)
	if err != nil {
		logger.Error("port dinlenemedi", "port", grpcPort, "error", err)
		os.Exit(1)
	}

	healthReporter.SetServing()

	go func() {
		logger.Info("gRPC sunucusu başlatıldı", "port", grpcPort)
		if err := grpcServer.Serve(lis); err != nil {
			logger.Error("gRPC server hatası", "error", err)
			os.Exit(1)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	logger.Info("kapatma sinyali alındı, graceful shutdown başlıyor")
	healthReporter.SetNotServing()
	time.Sleep(100 * time.Millisecond)
	grpcServer.GracefulStop()
	logger.Info("graceful shutdown tamamlandı")
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		logger.Error("zorunlu env değişkeni eksik", "key", key)
		os.Exit(1)
	}
	return v
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
