// Package idempotency, aynı isteğin (örn. bir ödeme çağrısının) birden
// fazla kez işlenmesini önlemek için Redis tabanlı bir kilit/sonuç-önbelleği
// mekanizması sağlar.
//
// Akış:
//  1. İstek gelir, Idempotency-Key VE istek gövdesi ile Acquire çağrılır.
//  2. Daha önce görülmemişse, çağıran tarafa BENZERSİZ bir "owner token"
//     verilir; iş mantığı bu token'ı elinde tutarak çalışır.
//  3. Sonuç Complete ile, SADECE bu owner token hâlâ geçerliyse (lease
//     süresi dolup başka bir worker key'i almadıysa) kalıcı hale getirilir.
//  4. Aynı key ile ikinci istek gelirse, Acquire saklanmış sonucu döner ve
//     iş mantığı TEKRAR ÇALIŞTIRILMAZ. Ancak istek gövdesi FARKLIYSA
//     (örn. aynı key farklı order/customer/amount ile kullanılmışsa),
//     ErrKeyReused döner -- önceki sonuç asla yanlış isteğe uygulanmaz.
//
// Sahiplik (ownership) neden gerekli? Eskiden Acquire, "in-progress"
// durumunu sabit 30 saniyelik bir TTL ile işaretliyordu ve Complete/Release
// bu kaydı kimin oluşturduğunu DOĞRULAMIYORDU. İşlem 30 saniyeyi aşarsa:
//   - İkinci bir istek aynı key'i "yeni" sanıp iş mantığını TEKRAR çalıştırabilir,
//   - İlk (yavaş) worker daha sonra tamamlanıp Complete çağırdığında, ikinci
//     worker'ın sonucunun ÜZERİNE YAZABİLİR ya da onu SİLEBİLİR (Release).
//
// Bu, çift ödeme/çift rezervasyon riskidir (bkz. rapor [K4]). Owner-token
// tabanlı lease bunu şu şekilde çözer: Complete/Release/RenewLease sadece
// hâlâ kaydı oluşturan worker'a aitse (owner eşleşirse) etki eder; aksi halde
// ErrLeaseLost döner ve çağıran taraf bunu kritik bir tutarsızlık olarak ele
// almalıdır (alert + manuel inceleme ya da retry).
package idempotency

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

var (
	// ErrInProgress, key şu anda başka bir worker tarafından işleniyorsa döner.
	ErrInProgress = errors.New("bu idempotency key için işlem hâlâ devam ediyor")

	// ErrKeyReused, aynı idempotency key farklı bir istek gövdesiyle
	// kullanılmaya çalışıldığında döner (bkz. rapor [K5]). Çağıran taraf
	// bunu codes.InvalidArgument gibi bir istemci hatasına çevirmelidir;
	// önceki sonuç ASLA bu isteğe uygulanmamalıdır.
	ErrKeyReused = errors.New("idempotency key farklı bir istek gövdesiyle yeniden kullanılıyor")

	// ErrLeaseLost, Complete/Release/RenewLease çağrılırken owner token
	// artık geçerli olmadığında (lease süresi dolmuş ve/veya başka bir
	// worker aynı key'i almış) döner. Bu KRİTİK bir durumdur: iş mantığı
	// tamamlanmış olabilir ama sonucu güvenle önbelleğe/telafi edilemez;
	// çağıran taraf loglamalı ve alert/manuel inceleme tetiklemelidir.
	ErrLeaseLost = errors.New("idempotency kilidinin sahipliği kaybedildi (lease süresi doldu)")
)

type Store struct {
	rdb       *redis.Client
	prefix    string
	resultTTL time.Duration
	leaseTTL  time.Duration
}

// NewStore, resultTTL=0 verilirse 24 saatlik varsayılan kalıcı sonuç TTL'si
// kullanır. In-flight lease TTL'si varsayılan olarak 2 dakikadır (bkz.
// WithLeaseTTL); bu, tek bir HTTP/gRPC timeout'undan daha uzun ama sonsuz
// olmayan, uzun süren işlemler için makul bir üst sınırdır -- gerçekten çok
// uzun süren işlemler RenewLease ile bu süreyi uzatmalıdır.
func NewStore(rdb *redis.Client, prefix string, resultTTL time.Duration) *Store {
	if resultTTL == 0 {
		resultTTL = 24 * time.Hour
	}
	return &Store{rdb: rdb, prefix: prefix, resultTTL: resultTTL, leaseTTL: 2 * time.Minute}
}

// WithLeaseTTL, in-flight kilidin TTL'sini özelleştirmek için kullanılır
// (örn. tipik olarak çok daha uzun süren işlemler için).
func (s *Store) WithLeaseTTL(d time.Duration) *Store {
	s.leaseTTL = d
	return s
}

func (s *Store) key(idempotencyKey string) string {
	return fmt.Sprintf("%s:idem:%s", s.prefix, idempotencyKey)
}

// hashRequest, verilen isteği (genellikle proto/JSON serileştirilebilir bir
// struct ya da map) kanonik bir SHA-256 hash'ine çevirir. Bu hash, aynı
// idempotency key'in farklı bir istek gövdesiyle yeniden kullanılmasını
// tespit etmek için saklanır (bkz. rapor [K5]). request nil ise (hash
// kontrolü istenmiyorsa) boş string döner.
func hashRequest(request any) (string, error) {
	if request == nil {
		return "", nil
	}
	data, err := json.Marshal(request)
	if err != nil {
		return "", fmt.Errorf("istek hash'lenemedi: %w", err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

// CheckResult, bir idempotency key'in durumunu ifade eder.
type CheckResult struct {
	// IsNew true ise bu key ilk kez görülüyor; çağıran taraf iş mantığını
	// çalıştırıp sonucu OwnerToken ile birlikte Complete'e vermelidir.
	IsNew bool
	// CachedResult, IsNew false ve işlem tamamlanmışsa daha önceki sonucu
	// içerir (JSON).
	CachedResult []byte
	// OwnerToken, IsNew true olduğunda bu Acquire çağrısına özel benzersiz
	// bir sahiplik token'ıdır. Complete/Release/RenewLease çağrılarında
	// AYNEN geri verilmelidir.
	OwnerToken string
}

// Lua scriptleri, Redis üzerinde atomik "kontrol-et-ve-yaz" (check-and-set)
// işlemleri için kullanılır; Go tarafında ayrı GET+SETNX adımları arasında
// oluşabilecek race condition'ları ortadan kaldırır. Redis'in gömülü cjson
// kütüphanesi basit JSON alan okuma/yazma için kullanılıyor.
var acquireScript = redis.NewScript(`
local key = KEYS[1]
local owner = ARGV[1]
local ttl_ms = tonumber(ARGV[2])
local req_hash = ARGV[3]

local raw = redis.call('GET', key)
if raw then
	local ok, wrapper = pcall(cjson.decode, raw)
	if ok then
		if req_hash ~= '' and wrapper.request_hash and wrapper.request_hash ~= '' and wrapper.request_hash ~= req_hash then
			return {'hash_mismatch'}
		end
		if wrapper.status == 'done' then
			return {'done', wrapper.data_raw}
		end
		return {'in_progress'}
	end
end

local newRaw = '{"status":"in_progress","owner":"' .. owner .. '","request_hash":"' .. req_hash .. '"}'
redis.call('SET', key, newRaw, 'PX', ttl_ms)
return {'new'}
`)

var completeScript = redis.NewScript(`
local key = KEYS[1]
local owner = ARGV[1]
local ttl_ms = tonumber(ARGV[2])
local data_raw = ARGV[3]

local raw = redis.call('GET', key)
if not raw then
	return 0
end
local ok, wrapper = pcall(cjson.decode, raw)
if not ok or wrapper.owner ~= owner then
	return 0
end

local req_hash = wrapper.request_hash or ''
local newRaw = '{"status":"done","request_hash":"' .. req_hash .. '","data_raw":' .. data_raw .. '}'
redis.call('SET', key, newRaw, 'PX', ttl_ms)
return 1
`)

var releaseScript = redis.NewScript(`
local key = KEYS[1]
local owner = ARGV[1]

local raw = redis.call('GET', key)
if not raw then
	return 1
end
local ok, wrapper = pcall(cjson.decode, raw)
if not ok then
	return 0
end
if wrapper.owner ~= owner then
	return 0
end
redis.call('DEL', key)
return 1
`)

var renewScript = redis.NewScript(`
local key = KEYS[1]
local owner = ARGV[1]
local ttl_ms = tonumber(ARGV[2])

local raw = redis.call('GET', key)
if not raw then
	return 0
end
local ok, wrapper = pcall(cjson.decode, raw)
if not ok or wrapper.owner ~= owner then
	return 0
end
redis.call('PEXPIRE', key, ttl_ms)
return 1
`)

// Acquire, key için bir "in-flight" kilidi almaya çalışır ve aynı zamanda
// key'in daha önce FARKLI bir istekle kullanılıp kullanılmadığını denetler
// (bkz. rapor [K5]). request, canonical hash üretmek için JSON'a
// serileştirilebilir olmalıdır (örn. sipariş/müşteri/tutar/kalemler gibi
// isteğin kimliğini belirleyen alanlar); request nil verilirse hash
// kontrolü atlanır.
func (s *Store) Acquire(ctx context.Context, idempotencyKey string, request any) (*CheckResult, error) {
	reqHash, err := hashRequest(request)
	if err != nil {
		return nil, err
	}

	owner := uuid.NewString()
	k := s.key(idempotencyKey)

	res, err := acquireScript.Run(ctx, s.rdb, []string{k}, owner, s.leaseTTL.Milliseconds(), reqHash).Result()
	if err != nil {
		return nil, fmt.Errorf("redis acquire scripti başarısız: %w", err)
	}

	arr, ok := res.([]any)
	if !ok || len(arr) == 0 {
		return nil, fmt.Errorf("beklenmeyen acquire script sonucu: %#v", res)
	}

	switch status, _ := arr[0].(string); status {
	case "hash_mismatch":
		return nil, ErrKeyReused
	case "done":
		var dataRaw string
		if len(arr) > 1 {
			dataRaw, _ = arr[1].(string)
		}
		return &CheckResult{IsNew: false, CachedResult: []byte(dataRaw)}, nil
	case "in_progress":
		return nil, ErrInProgress
	case "new":
		return &CheckResult{IsNew: true, OwnerToken: owner}, nil
	default:
		return nil, fmt.Errorf("bilinmeyen acquire script durumu: %q", status)
	}
}

// Complete, iş mantığı tamamlandıktan sonra sonucu kalıcı TTL ile saklar.
// ownerToken, Acquire'dan dönen CheckResult.OwnerToken ile AYNI olmalıdır;
// lease süresi dolup başka bir worker key'i almışsa bu çağrı ErrLeaseLost
// döner ve sonuç YAZILMAZ (bkz. rapor [K4] -- eski worker'ın yeni worker'ın
// sonucunu ezmesi engellenir).
func (s *Store) Complete(ctx context.Context, idempotencyKey, ownerToken string, result any) error {
	data, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("sonuç marshal edilemedi: %w", err)
	}
	// data_raw doğrudan Lua script'i içine gömüleceği için JSON içindeki
	// tek tırnak/kaçış karakterlerinden etkilenmemesi adına string olarak
	// değil, olduğu gibi (geçerli bir JSON literali olarak) gömülüyor.
	k := s.key(idempotencyKey)
	res, err := completeScript.Run(ctx, s.rdb, []string{k}, ownerToken, s.resultTTL.Milliseconds(), string(data)).Result()
	if err != nil {
		return fmt.Errorf("redis complete scripti başarısız: %w", err)
	}
	if n, _ := res.(int64); n != 1 {
		return ErrLeaseLost
	}
	return nil
}

// Release, iş mantığı hata ile başarısız olduğunda kilidi serbest bırakır
// ki aynı key ile yeniden deneme (retry) mümkün olsun. Sadece ownerToken
// hâlâ geçerliyse (bu worker hâlâ kilidin sahibiyse) silme işlemi yapılır.
func (s *Store) Release(ctx context.Context, idempotencyKey, ownerToken string) error {
	k := s.key(idempotencyKey)
	res, err := releaseScript.Run(ctx, s.rdb, []string{k}, ownerToken).Result()
	if err != nil {
		return fmt.Errorf("redis release scripti başarısız: %w", err)
	}
	if n, _ := res.(int64); n != 1 {
		return ErrLeaseLost
	}
	return nil
}

// RenewLease, uzun süren işlemler için in-flight kilidin TTL'sini uzatır.
// Çağıran taraf (örn. beklenenden uzun süren bir dış sağlayıcı çağrısı
// sırasında) periyodik olarak bunu çağırarak lease'in süresinin dolup
// başka bir worker'ın aynı key'i "yeni" sanmasını engelleyebilir.
func (s *Store) RenewLease(ctx context.Context, idempotencyKey, ownerToken string) error {
	k := s.key(idempotencyKey)
	res, err := renewScript.Run(ctx, s.rdb, []string{k}, ownerToken, s.leaseTTL.Milliseconds()).Result()
	if err != nil {
		return fmt.Errorf("redis renew scripti başarısız: %w", err)
	}
	if n, _ := res.(int64); n != 1 {
		return ErrLeaseLost
	}
	return nil
}

// sanitizeForLua, gömülü Lua script literallerine tek tırnak/backslash
// içeren değerlerin (bu pakette owner UUID/hash olduğu için pratikte hiç
// olmaz) kaçmadan sızmasını önlemek için kullanılabilecek bir yardımcıdır.
// Şu an owner=uuid.NewString() ve req_hash=hex.EncodeToString(...) her
// zaman [0-9a-f-] karakter kümesiyle sınırlı olduğu için ekstra kaçışa
// gerek yoktur; bu fonksiyon gelecekte farklı bir owner/kimlik üretim
// stratejisine geçilirse savunma amaçlı burada tutulur.
func sanitizeForLua(s string) string {
	replacer := strings.NewReplacer(`\`, `\\`, `'`, `\'`)
	return replacer.Replace(s)
}
