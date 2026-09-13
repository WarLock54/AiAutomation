// Package resilience, order-service'in inventory-service/payment-service'e
// yaptığı gRPC çağrılarına dayanıklılık (resilience) katmanı ekler:
//
//  1. Retry (üstel geri çekilme): sadece GEÇİCİ hatalarda (Unavailable,
//     DeadlineExceeded, ResourceExhausted) otomatik tekrar dener. İş
//     mantığı hataları (InvalidArgument, FailedPrecondition vb.) ASLA
//     retry edilmez -- aksi halde yanlış sonuçlara yol açabilir.
//  2. Circuit Breaker: hedef servis art arda başarısız olduğunda devreyi
//     açar ve bir süre çağrıyı denemeden hızlıca başarısız olur ("fail
//     fast"), böylece çökmüş bir downstream servise karşı order-service
//     kendi kaynaklarını (goroutine, bağlantı havuzu) tüketmez.
//
// Bu, klasik .NET/Polly "retry + circuit breaker" pattern'inin gRPC
// istemci interceptor'ları ile Go karşılığıdır.
package resilience

import (
	"context"
	"time"

	"github.com/sony/gobreaker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RetryConfig, üstel geri çekilmeli retry davranışını tanımlar.
type RetryConfig struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
}

func DefaultRetryConfig() RetryConfig {
	return RetryConfig{MaxAttempts: 3, BaseDelay: 100 * time.Millisecond, MaxDelay: 2 * time.Second}
}

// isRetryable, sadece ağ/aşırı yük kaynaklı geçici hataların tekrar
// denenmesine izin verir.
func isRetryable(err error) bool {
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	switch st.Code() {
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted:
		return true
	default:
		return false
	}
}

// RetryUnaryClientInterceptor, geçici hatalarda üstel geri çekilmeyle
// (exponential backoff) çağrıyı tekrar dener. Context iptal/deadline'ına
// saygı gösterir (retry sırasında context bitmişse hemen döner).
func RetryUnaryClientInterceptor(cfg RetryConfig) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		var lastErr error
		delay := cfg.BaseDelay

		for attempt := 1; attempt <= cfg.MaxAttempts; attempt++ {
			lastErr = invoker(ctx, method, req, reply, cc, opts...)
			if lastErr == nil || !isRetryable(lastErr) {
				return lastErr
			}
			if attempt == cfg.MaxAttempts {
				break
			}

			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return ctx.Err()
			}

			delay *= 2
			if delay > cfg.MaxDelay {
				delay = cfg.MaxDelay
			}
		}

		return lastErr
	}
}

// CircuitBreakerUnaryClientInterceptor, art arda 5 başarısızlıktan sonra
// devreyi açar; 10 saniye boyunca çağrılar denenmeden Unavailable hatası
// döner, sonra "half-open" durumda birkaç deneme yapılıp devre kapatılır
// ya da tekrar açılır.
func CircuitBreakerUnaryClientInterceptor(name string) grpc.UnaryClientInterceptor {
	cb := gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        name,
		MaxRequests: 3,
		Interval:    30 * time.Second,
		Timeout:     10 * time.Second,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= 5
		},
	})

	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		_, err := cb.Execute(func() (interface{}, error) {
			return nil, invoker(ctx, method, req, reply, cc, opts...)
		})
		if err == gobreaker.ErrOpenState || err == gobreaker.ErrTooManyRequests {
			return status.Error(codes.Unavailable, "devre açık (circuit breaker): hedef servis şu an çağrılmıyor")
		}
		return err
	}
}
