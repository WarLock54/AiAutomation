// Package grpcmiddleware, tüm servislerin ortak kullandığı gRPC unary
// interceptor'larını içerir: request logging, panic recovery ve ileride
// OpenTelemetry span'lerinin bağlanacağı tracing hook'u.
//
// Proje 3'te (Observability & GitOps) buraya otelgrpc interceptor'ı
// eklenerek dağıtık izleme (distributed tracing) tamamlanacak. Şimdilik
// context'e bir correlation ID enjekte ediyoruz ki log satırları arasında
// bir isteği uçtan uca takip edebilelim.
package grpcmiddleware

import (
	"context"
	"log"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type correlationIDKey struct{}

// CorrelationIDFromContext, LoggingInterceptor tarafından enjekte edilen
// correlation ID'yi döner. Bulunamazsa boş string döner.
func CorrelationIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(correlationIDKey{}).(string); ok {
		return v
	}
	return ""
}

// LoggingInterceptor, her gelen çağrı için bir correlation ID üretir
// (istemci zaten "x-correlation-id" metadata'sı gönderdiyse onu kullanır),
// süresini ölçer ve sonucu loglar.
func LoggingInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		correlationID := extractOrGenerateCorrelationID(ctx)
		ctx = context.WithValue(ctx, correlationIDKey{}, correlationID)

		start := time.Now()
		resp, err := handler(ctx, req)
		duration := time.Since(start)

		if err != nil {
			log.Printf("[%s] method=%s duration=%s status=ERROR err=%v",
				correlationID, info.FullMethod, duration, err)
		} else {
			log.Printf("[%s] method=%s duration=%s status=OK",
				correlationID, info.FullMethod, duration)
		}
		return resp, err
	}
}

// RecoveryInterceptor, handler içindeki panic'leri yakalayıp servisi ayakta
// tutar; panic'i gRPC internal error'a çevirir.
func RecoveryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[PANIC] method=%s recovered=%v", info.FullMethod, r)
				err = status.Error(codes.Internal, "beklenmeyen bir sunucu hatası oluştu")
			}
		}()
		return handler(ctx, req)
	}
}

func extractOrGenerateCorrelationID(ctx context.Context) string {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if vals := md.Get("x-correlation-id"); len(vals) > 0 && vals[0] != "" {
			return vals[0]
		}
	}
	return uuid.NewString()
}
