package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"

	inventoryv1 "order-engine/gen/inventory/v1"
	orderv1 "order-engine/gen/order/v1"
	paymentv1 "order-engine/gen/payment/v1"
	"order-engine/pkg/grpcmiddleware"
	"order-engine/pkg/health"
	"order-engine/pkg/kafka"
	"order-engine/pkg/outbox"
	"order-engine/pkg/resilience"
	"order-engine/pkg/tracing"
	"order-engine/services/order-service/internal"
)

// logger, JSON formatında yapısal loglama sağlar (log/slog, ek bağımlılık
// gerektirmez). Her log satırı service=order-service alanını taşır ki
// merkezi bir log toplayıcıda (örn. Loki/ELK, bkz. Proje 3) servisler
// birbirinden kolayca ayrıştırılabilsin.
var logger = slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "order-service")

func main() {
	grpcPort := mustEnv("GRPC_PORT")
	kafkaBrokers := []string{mustEnv("KAFKA_BROKERS")}
	postgresDSN := mustEnv("POSTGRES_DSN")
	inventoryAddr := mustEnv("INVENTORY_SERVICE_ADDR")
	paymentAddr := mustEnv("PAYMENT_SERVICE_ADDR")

	// Distributed Tracing: order-service, Saga'nın başladığı yer olduğu
	// için trace'in KÖKÜ (root span) burada oluşur. otelgrpc client
	// handler'ları (aşağıda), bu trace context'ini downstream gRPC
	// çağrılarına (inventory/payment-service) otomatik olarak ekler.
	shutdownTracing, err := tracing.Init(context.Background(), "order-service", envOrDefault("OTLP_ENDPOINT", "jaeger:4317"))
	if err != nil {
		logger.Warn("tracing başlatılamadı, izleme olmadan devam ediliyor", "error", err)
	} else {
		defer shutdownTracing(context.Background())
	}

	// --- Downstream gRPC client bağlantıları ---
	// Her istemciye circuit breaker (dış) + retry (iç) interceptor zinciri
	// eklendi: breaker sadece retry'lar tükendikten SONRAKİ nihai sonucu
	// sayar, böylece geçici bir gecikmeyi devreyi açmakla karıştırmaz.
	// otelgrpc StatsHandler, trace context'ini bu çağrılara ekleyip
	// span üretir.
	inventoryConn, err := grpc.NewClient(inventoryAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
		grpc.WithChainUnaryInterceptor(
			resilience.CircuitBreakerUnaryClientInterceptor("inventory-service"),
			resilience.RetryUnaryClientInterceptor(resilience.DefaultRetryConfig()),
		),
	)
	if err != nil {
		logger.Error("inventory-service'e bağlanılamadı", "error", err)
		os.Exit(1)
	}
	defer inventoryConn.Close()
	inventoryClient := inventoryv1.NewInventoryServiceClient(inventoryConn)

	paymentConn, err := grpc.NewClient(paymentAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
		grpc.WithChainUnaryInterceptor(
			resilience.CircuitBreakerUnaryClientInterceptor("payment-service"),
			resilience.RetryUnaryClientInterceptor(resilience.DefaultRetryConfig()),
		),
	)
	if err != nil {
		logger.Error("payment-service'e bağlanılamadı", "error", err)
		os.Exit(1)
	}
	defer paymentConn.Close()
	paymentClient := paymentv1.NewPaymentServiceClient(paymentConn)

	repo, err := internal.NewPostgresRepository(postgresDSN)
	if err != nil {
		logger.Error("postgres bağlantısı kurulamadı", "error", err)
		os.Exit(1)
	}
	defer repo.Close()

	producer := kafka.NewProducer(kafkaBrokers, "order-events")
	defer producer.Close()

	// Transactional Outbox Publisher: order-service artık Execute/Finalize
	// içinde DOĞRUDAN Kafka'ya publish etmiyor (bkz. rapor [K6]); sipariş
	// durumu güncellemesiyle aynı transaction'da outbox_events tablosuna
	// yazılan event'ler burada, ayrı bir arka plan döngüsünde, en-az-bir-kez
	// garantisiyle yayınlanır.
	outboxCtx, cancelOutbox := context.WithCancel(context.Background())
	defer cancelOutbox()
	outboxPublisher := outbox.NewPublisher(repo.Pool(), producer, outbox.DefaultPublisherConfig(), logger.With("component", "outbox-publisher"))
	go outboxPublisher.Run(outboxCtx)

	// Saga Orchestrator: sipariş oluşturma akışının her adımını sırayla
	// yürütür ve hata durumunda telafi edici (compensating) adımları tetikler.
	orchestrator := internal.NewOrchestrator(inventoryClient, paymentClient, repo)
	svc := internal.NewOrderServer(repo, orchestrator)

	grpcServer := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.ChainUnaryInterceptor(
			grpcmiddleware.RecoveryInterceptor(),
			grpcmiddleware.LoggingInterceptor(),
		),
	)
	orderv1.RegisterOrderServiceServer(grpcServer, svc)

	// gRPC Health Checking Protocol: K8s liveness/readiness probe'ları
	// (grpc_health_probe) bu servisi sorgulayabilir (bkz. Proje 3).
	healthReporter := health.Register(grpcServer, "order.v1.OrderService")

	// grpcurl gibi araçların .proto dosyası vermeden servisleri
	// keşfedebilmesi (list/describe/health check) için reflection açık.
	reflection.Register(grpcServer)

	lis, err := net.Listen("tcp", ":"+grpcPort)
	if err != nil {
		logger.Error("port dinlenemedi", "port", grpcPort, "error", err)
		os.Exit(1)
	}

	// Tüm bağımlılıklar (Postgres, Kafka, downstream gRPC client'lar)
	// hazır olduğuna göre servisi SERVING olarak işaretle.
	healthReporter.SetServing()

	// Saga State Machine Recovery: yarım kalmış (crash/restart nedeniyle
	// terminal olmayan durumda takılı kalmış) siparişleri arka planda,
	// PERİYODİK olarak ve claim/lease deseniyle kurtarır (bkz. rapor
	// [K7]). Arka planda çalıştırılıyor ki health check hemen SERVING
	// dönebilsin (K8s rolling update sırasında pod'un "hazır" görünmesi
	// gecikmesin). recoveryCtx, graceful shutdown sırasında iptal edilir.
	recoveryCtx, cancelRecovery := context.WithCancel(context.Background())
	defer cancelRecovery()
	go orchestrator.RunRecoveryLoop(recoveryCtx, internal.DefaultRecoveryConfig())

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
	// K8s'e "artık bana yeni istek gönderme" sinyali - trafiğin durması
	// için readiness probe'un bunu görmesine kısa bir süre tanınır.
	healthReporter.SetNotServing()
	time.Sleep(100 * time.Millisecond)
	grpcServer.GracefulStop()
	cancelOutbox()
	cancelRecovery()
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
