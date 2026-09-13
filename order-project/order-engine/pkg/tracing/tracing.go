// Package tracing, OpenTelemetry ile dağıtık izleme (distributed tracing)
// kurulumunu tek bir yerde toplar. Her servis kendi main.go'sunda
// tracing.Init(...) çağırarak bir TracerProvider kaydeder; ardından
// pkg/grpcmiddleware ile birlikte kullanılan otelgrpc stats handler'ları
// (bkz. her main.go) her gRPC çağrısı için otomatik olarak span üretip
// trace context'i (W3C traceparent) servisler arasında propagate eder.
//
// Sonuç: order-service'te başlayan bir CreateOrder isteği, inventory-service
// ve payment-service'e yapılan gRPC çağrılarıyla TEK BİR trace altında
// birleşir. Bu, Saga akışının hangi adımının ne kadar sürdüğünü ve
// hatanın tam olarak hangi serviste oluştuğunu Jaeger UI'da (bkz.
// docker-compose.yml, http://localhost:16686) uçtan uca görmeyi sağlar.
package tracing

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Init, verilen servis adıyla bir OpenTelemetry TracerProvider kurar ve
// span'ları OTLP/gRPC üzerinden otlpEndpoint'e (bu projede Jaeger'ın OTLP
// alıcısı, "jaeger:4317") gönderir.
//
// Dönen shutdown fonksiyonu, servis graceful shutdown'da bekleyen span'ların
// flush edilmesi için mutlaka çağrılmalıdır -- aksi halde son birkaç
// saniyedeki trace'ler kaybolabilir.
func Init(ctx context.Context, serviceName, otlpEndpoint string) (shutdown func(context.Context) error, err error) {
	conn, err := grpc.NewClient(otlpEndpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("otlp collector'a bağlanılamadı: %w", err)
	}

	exporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithGRPCConn(conn))
	if err != nil {
		return nil, fmt.Errorf("otlp exporter oluşturulamadı: %w", err)
	}

	// NOT: resource.Default() + resource.Merge(...) yerine bilinçli olarak
	// NewSchemaless kullanılıyor. go.mod'daki otel/sdk ve semconv paketlerinin
	// versiyonları `go mod tidy` sırasında birbirinden bağımsız güncellenebiliyor;
	// bu durumda resource.Default()'un kendi içindeki şema URL'i ile burada
	// elle import edilen semconv paketinin şema URL'i uyuşmayabilir ve
	// resource.Merge "conflicting Schema URL" hatasıyla başarısız olur.
	// Schemaless bir resource bu sınıf hataları tamamen ortadan kaldırır;
	// bedeli sadece resource.Default()'un otomatik eklediği process/host/OS
	// bilgilerinden feragat etmek (bu proje için gerekli değiller, tek
	// ihtiyacımız olan "service.name" etiketi zaten burada var).
	res := resource.NewSchemaless(
		semconv.ServiceName(serviceName),
	)

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter, sdktrace.WithBatchTimeout(2*time.Second)),
		sdktrace.WithResource(res),
	)

	otel.SetTracerProvider(tp)
	// W3C Trace Context (traceparent header) + Baggage: gRPC metadata
	// üzerinden servisler arası trace ID/span ID propagation'ı bunlarla
	// yapılır (otelgrpc stats handler'ları bu propagator'ı kullanır).
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return tp.Shutdown, nil
}
