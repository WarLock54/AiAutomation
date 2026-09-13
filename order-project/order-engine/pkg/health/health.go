// Package health, standart gRPC Health Checking Protocol'ü
// (grpc.health.v1.Health) sarmalayarak servislerin K8s liveness/readiness
// probe'ları tarafından sorgulanabilmesini sağlar (bkz. Proje 3).
//
// Kullanım: servis DB/Kafka/Redis'e bağlanana kadar durum NOT_SERVING'dir;
// bağımlılıklar hazır olduğunda SetServing() çağrılır. Böylece K8s, henüz
// trafiğe hazır olmayan bir pod'a istek yönlendirmez.
package health

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

type Reporter struct {
	server      *health.Server
	serviceName string
}

// Register, verilen gRPC server'a health servisini ekler ve başlangıç
// durumunu NOT_SERVING olarak ayarlar (henüz bağımlılıklar hazır değil).
func Register(grpcServer *grpc.Server, serviceName string) *Reporter {
	hs := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, hs)

	r := &Reporter{server: hs, serviceName: serviceName}
	hs.SetServingStatus(serviceName, healthpb.HealthCheckResponse_NOT_SERVING)
	// Boş servis adı da (grpc_health_probe'un varsayılan sorgusu) aynı
	// duruma ayarlanır.
	hs.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	return r
}

// SetServing, tüm bağımlılıklar (Postgres/Kafka/Redis) hazır olduğunda
// çağrılır ve servisi SERVING durumuna geçirir.
func (r *Reporter) SetServing() {
	r.server.SetServingStatus(r.serviceName, healthpb.HealthCheckResponse_SERVING)
	r.server.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
}

// SetNotServing, graceful shutdown başladığında çağrılır ki K8s yeni
// istekleri bu pod'a yönlendirmeyi bıraksın.
func (r *Reporter) SetNotServing() {
	r.server.SetServingStatus(r.serviceName, healthpb.HealthCheckResponse_NOT_SERVING)
	r.server.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
}
