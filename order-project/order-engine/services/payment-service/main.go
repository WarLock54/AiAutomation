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

	paymentv1 "order-engine/gen/payment/v1"
	"order-engine/pkg/grpcmiddleware"
	"order-engine/pkg/health"
	"order-engine/pkg/idempotency"
	"order-engine/pkg/kafka"
	"order-engine/pkg/outbox"
	"order-engine/pkg/tracing"
	"order-engine/services/payment-service/internal"
)

var logger = slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "payment-service")

func main() {
	grpcPort := mustEnv("GRPC_PORT")
	kafkaBrokers := []string{mustEnv("KAFKA_BROKERS")}
	postgresDSN := mustEnv("POSTGRES_DSN")
	redisAddr := mustEnv("REDIS_ADDR")

	// Distributed Tracing: order-service'ten gelen trace context'i
	// (traceparent) otomatik olarak devralınır -- otelgrpc StatsHandler
	// bunu grpc.NewServer'a eklendiğinde yönetir (aşağıda).
	shutdownTracing, err := tracing.Init(context.Background(), "payment-service", envOrDefault("OTLP_ENDPOINT", "jaeger:4317"))
	if err != nil {
		logger.Warn("tracing başlatılamadı, izleme olmadan devam ediliyor", "error", err)
	} else {
		defer shutdownTracing(context.Background())
	}

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()

	idemStore := idempotency.NewStore(rdb, "payment", 24*time.Hour)

	repo, err := internal.NewPostgresRepository(postgresDSN)
	if err != nil {
		logger.Error("postgres bağlantısı kurulamadı", "error", err)
		os.Exit(1)
	}
	defer repo.Close()

	producer := kafka.NewProducer(kafkaBrokers, "payment-events")
	defer producer.Close()

	// Transactional Outbox Publisher: payment-service artık ProcessPayment
	// içinde DOĞRUDAN Kafka'ya publish etmiyor (bkz. rapor [K6]); ödeme
	// yazımıyla aynı transaction'da outbox_events tablosuna yazılan
	// event'ler burada, ayrı bir arka plan döngüsünde, en-az-bir-kez
	// garantisiyle ve üstel geri-çekilme ile yayınlanır. Bu goroutine
	// birden fazla payment-service instance'ında aynı anda çalışabilir;
	// `FOR UPDATE SKIP LOCKED` sayesinde aynı event iki instance
	// tarafından birden yayınlanmaz.
	outboxCtx, cancelOutbox := context.WithCancel(context.Background())
	defer cancelOutbox()
	outboxPublisher := outbox.NewPublisher(repo.Pool(), producer, outbox.DefaultPublisherConfig(), logger.With("component", "outbox-publisher"))
	go outboxPublisher.Run(outboxCtx)

	svc := internal.NewPaymentServer(repo, idemStore)

	grpcServer := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.ChainUnaryInterceptor(
			grpcmiddleware.RecoveryInterceptor(),
			grpcmiddleware.LoggingInterceptor(),
		),
	)
	paymentv1.RegisterPaymentServiceServer(grpcServer, svc)

	// gRPC Health Checking Protocol: K8s liveness/readiness probe'ları
	// bu servisi sorgulayabilir (bkz. Proje 3).
	healthReporter := health.Register(grpcServer, "payment.v1.PaymentService")

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

	waitForShutdown(func(ctx context.Context) {
		healthReporter.SetNotServing()
		time.Sleep(100 * time.Millisecond)
		grpcServer.GracefulStop()
		cancelOutbox()
	})
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

func waitForShutdown(shutdown func(ctx context.Context)) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	logger.Info("kapatma sinyali alındı, graceful shutdown başlıyor")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	shutdown(ctx)
	logger.Info("graceful shutdown tamamlandı")
}
