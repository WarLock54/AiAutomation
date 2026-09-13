// Package idempotency, aynÄ± isteÄŸin (Ã¶rn. bir Ã¶deme Ã§aÄŸrÄ±sÄ±nÄ±n) birden
// fazla kez iÅŸlenmesini Ã¶nlemek iÃ§in Redis tabanlÄ± bir kilit/sonuÃ§-Ã¶nbelleÄŸi
// mekanizmasÄ± saÄŸlar.
//
// AkÄ±ÅŸ:
//  1. Ä°stek gelir, Idempotency-Key VE istek gÃ¶vdesi ile Acquire Ã§aÄŸrÄ±lÄ±r.
//  2. Daha Ã¶nce gÃ¶rÃ¼lmemiÅŸse, Ã§aÄŸÄ±ran tarafa BENZERSÄ°Z bir "owner token"
//     verilir; iÅŸ mantÄ±ÄŸÄ± bu token'Ä± elinde tutarak Ã§alÄ±ÅŸÄ±r.
//  3. SonuÃ§ Complete ile, SADECE bu owner token hÃ¢lÃ¢ geÃ§erliyse (lease
//     sÃ¼resi dolup baÅŸka bir worker key'i almadÄ±ysa) kalÄ±cÄ± hale getirilir.
//  4. AynÄ± key ile ikinci istek gelirse, Acquire saklanmÄ±ÅŸ sonucu dÃ¶ner ve
//     iÅŸ mantÄ±ÄŸÄ± TEKRAR Ã‡ALIÅTIRILMAZ. Ancak istek gÃ¶vdesi FARKLIYSA
//     (Ã¶rn. aynÄ± key farklÄ± order/customer/amount ile kullanÄ±lmÄ±ÅŸsa),
//     ErrKeyReused dÃ¶ner -- Ã¶nceki sonuÃ§ asla yanlÄ±ÅŸ isteÄŸe uygulanmaz.
//
// Sahiplik (ownership) neden gerekli? Eskiden Acquire, "in-progress"
// durumunu sabit 30 saniyelik bir TTL ile iÅŸaretliyordu ve Complete/Release
// bu kaydÄ± kimin oluÅŸturduÄŸunu DOÄRULAMIYORDU. Ä°ÅŸlem 30 saniyeyi aÅŸarsa:
//   - Ä°kinci bir istek aynÄ± key'i "yeni" sanÄ±p iÅŸ mantÄ±ÄŸÄ±nÄ± TEKRAR Ã§alÄ±ÅŸtÄ±rabilir,
//   - Ä°lk (yavaÅŸ) worker daha sonra tamamlanÄ±p Complete Ã§aÄŸÄ±rdÄ±ÄŸÄ±nda, ikinci
//     worker'Ä±n sonucunun ÃœZERÄ°NE YAZABÄ°LÄ°R ya da onu SÄ°LEBÄ°LÄ°R (Release).
// Bu, Ã§ift Ã¶deme/Ã§ift rezervasyon riskidir (bkz. rapor [K4]). Owner-token
// tabanlÄ± lease bunu ÅŸu ÅŸekilde Ã§Ã¶zer: Complete/Release/RenewLease sadece
// hÃ¢lÃ¢ kaydÄ± oluÅŸturan worker'a aitse (owner eÅŸleÅŸirse) etki eder; aksi halde
// ErrLeaseLost dÃ¶ner ve Ã§aÄŸÄ±ran taraf bunu kritik bir tutarsÄ±zlÄ±k olarak ele
// almalÄ±dÄ±r (alert + manuel inceleme ya da retry).
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
	// ErrInProgress, key ÅŸu anda baÅŸka bir worker tarafÄ±ndan iÅŸleniyorsa dÃ¶ner.
	ErrInProgress = errors.New("bu idempotency key iÃ§in iÅŸlem hÃ¢lÃ¢ devam ediyor")

	// ErrKeyReused, aynÄ± idempotency key farklÄ± bir istek gÃ¶vdesiyle
	// kullanÄ±lmaya Ã§alÄ±ÅŸÄ±ldÄ±ÄŸÄ±nda dÃ¶ner (bkz. rapor [K5]). Ã‡aÄŸÄ±ran taraf
	// bunu codes.InvalidArgument gibi bir istemci hatasÄ±na Ã§evirmelidir;
	// Ã¶nceki sonuÃ§ ASLA bu isteÄŸe uygulanmamalÄ±dÄ±r.
	ErrKeyReused = errors.New("idempotency key farklÄ± bir istek gÃ¶vdesiyle yeniden kullanÄ±lÄ±yor")

	// ErrLeaseLost, Complete/Release/RenewLease Ã§aÄŸrÄ±lÄ±rken owner token
	// artÄ±k geÃ§erli olmadÄ±ÄŸÄ±nda (lease sÃ¼resi dolmuÅŸ ve/veya baÅŸka bir
	// worker aynÄ± key'i almÄ±ÅŸ) dÃ¶ner. Bu KRÄ°TÄ°K bir durumdur: iÅŸ mantÄ±ÄŸÄ±
	// tamamlanmÄ±ÅŸ olabilir ama sonucu gÃ¼venle Ã¶nbelleÄŸe/telafi edilemez;
	// Ã§aÄŸÄ±ran taraf loglamalÄ± ve alert/manuel inceleme tetiklemelidir.
	ErrLeaseLost = errors.New("idempotency kilidinin sahipliÄŸi kaybedildi (lease sÃ¼resi doldu)")
)

type Store struct {
	rdb       *redis.Client
	prefix    string
	resultTTL time.Duration
	leaseTTL  time.Duration
}

// NewStore, resultTTL=0 verilirse 24 saatlik varsayÄ±lan kalÄ±cÄ± sonuÃ§ TTL'si
// kullanÄ±r. In-flight lease TTL'si varsayÄ±lan olarak 2 dakikadÄ±r (bkz.
// WithLeaseTTL); bu, tek bir HTTP/gRPC timeout'undan daha uzun ama sonsuz
// olmayan, uzun sÃ¼ren iÅŸlemler iÃ§in makul bir Ã¼st sÄ±nÄ±rdÄ±r -- gerÃ§ekten Ã§ok
// uzun sÃ¼ren iÅŸlemler RenewLease ile bu sÃ¼reyi uzatmalÄ±dÄ±r.
func NewStore(rdb *redis.Client, prefix string, resultTTL time.Duration) *Store {
	if resultTTL == 0 {
		resultTTL = 24 * time.Hour
	}
	return &Store{rdb: rdb, prefix: prefix, resultTTL: resultTTL, leaseTTL: 2 * time.Minute}
}

// WithLeaseTTL, in-flight kilidin TTL'sini Ã¶zelleÅŸtirmek iÃ§in kullanÄ±lÄ±r
// (Ã¶rn. tipik olarak Ã§ok daha uzun sÃ¼ren iÅŸlemler iÃ§in).
func (s *Store) WithLeaseTTL(d time.Duration) *Store {
	s.leaseTTL = d
	return s
}

func (s *Store) key(idempotencyKey string) string {
	return fmt.Sprintf("%s:idem:%s", s.prefix, idempotencyKey)
}

// hashRequest, verilen isteÄŸi (genellikle proto/JSON serileÅŸtirilebilir bir
// struct ya da map) kanonik bir SHA-256 hash'ine Ã§evirir. Bu hash, aynÄ±
// idempotency key'in farklÄ± bir istek gÃ¶vdesiyle yeniden kullanÄ±lmasÄ±nÄ±
// tespit etmek iÃ§in saklanÄ±r (bkz. rapor [K5]). request nil ise (hash
// kontrolÃ¼ istenmiyorsa) boÅŸ string dÃ¶ner.
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
	// IsNew true ise bu key ilk kez gÃ¶rÃ¼lÃ¼yor; Ã§aÄŸÄ±ran taraf iÅŸ mantÄ±ÄŸÄ±nÄ±
	// Ã§alÄ±ÅŸtÄ±rÄ±p sonucu OwnerToken ile birlikte Complete'e vermelidir.
	IsNew bool
	// CachedResult, IsNew false ve iÅŸlem tamamlanmÄ±ÅŸsa daha Ã¶nceki sonucu
	// iÃ§erir (JSON).
	CachedResult []byte
	// OwnerToken, IsNew true olduÄŸunda bu Acquire Ã§aÄŸrÄ±sÄ±na Ã¶zel benzersiz
	// bir sahiplik token'Ä±dÄ±r. Complete/Release/RenewLease Ã§aÄŸrÄ±larÄ±nda
	// AYNEN geri verilmelidir.
	OwnerToken string
}

// Lua scriptleri, Redis Ã¼zerinde atomik "kontrol-et-ve-yaz" (check-and-set)
// iÅŸlemleri iÃ§in kullanÄ±lÄ±r; Go tarafÄ±nda ayrÄ± GET+SETNX adÄ±mlarÄ± arasÄ±nda
// oluÅŸabilecek race condition'larÄ± ortadan kaldÄ±rÄ±r. Redis'in gÃ¶mÃ¼lÃ¼ cjson
// kÃ¼tÃ¼phanesi basit JSON alan okuma/yazma iÃ§in kullanÄ±lÄ±yor.
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

// Acquire, key iÃ§in bir "in-flight" kilidi almaya Ã§alÄ±ÅŸÄ±r ve aynÄ± zamanda
// key'in daha Ã¶nce FARKLI bir istekle kullanÄ±lÄ±p kullanÄ±lmadÄ±ÄŸÄ±nÄ± denetler
// (bkz. rapor [K5]). request, canonical hash Ã¼retmek iÃ§in JSON'a
// serileÅŸtirilebilir olmalÄ±dÄ±r (Ã¶rn. sipariÅŸ/mÃ¼ÅŸteri/tutar/kalemler gibi
// isteÄŸin kimliÄŸini belirleyen alanlar); request nil verilirse hash
// kontrolÃ¼ atlanÄ±r.
func (s *Store) Acquire(ctx context.Context, idempotencyKey string, request any) (*CheckResult, error) {
	reqHash, err := hashRequest(request)
	if err != nil {
		return nil, err
	}

	owner := uuid.NewString()
	k := s.key(idempotencyKey)

	res, err := acquireScript.Run(ctx, s.rdb, []string{k}, owner, s.leaseTTL.Milliseconds(), reqHash).Result()
	if err != nil {
		return nil, fmt.Errorf("redis acquire scripti baÅŸarÄ±sÄ±z: %w", err)
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

// Complete, iÅŸ mantÄ±ÄŸÄ± tamamlandÄ±ktan sonra sonucu kalÄ±cÄ± TTL ile saklar.
// ownerToken, Acquire'dan dÃ¶nen CheckResult.OwnerToken ile AYNI olmalÄ±dÄ±r;
// lease sÃ¼resi dolup baÅŸka bir worker key'i almÄ±ÅŸsa bu Ã§aÄŸrÄ± ErrLeaseLost
// dÃ¶ner ve sonuÃ§ YAZILMAZ (bkz. rapor [K4] -- eski worker'Ä±n yeni worker'Ä±n
// sonucunu ezmesi engellenir).
func (s *Store) Complete(ctx context.Context, idempotencyKey, ownerToken string, result any) error {
	data, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("sonuÃ§ marshal edilemedi: %w", err)
	}
	// data_raw doÄŸrudan Lua script'i iÃ§ine gÃ¶mÃ¼leceÄŸi iÃ§in JSON iÃ§indeki
	// tek tÄ±rnak/kaÃ§Ä±ÅŸ karakterlerinden etkilenmemesi adÄ±na string olarak
	// deÄŸil, olduÄŸu gibi (geÃ§erli bir JSON literali olarak) gÃ¶mÃ¼lÃ¼yor.
	k := s.key(idempotencyKey)
	res, err := completeScript.Run(ctx, s.rdb, []string{k}, ownerToken, s.resultTTL.Milliseconds(), string(data)).Result()
	if err != nil {
		return fmt.Errorf("redis complete scripti baÅŸarÄ±sÄ±z: %w", err)
	}
	if n, _ := res.(int64); n != 1 {
		return ErrLeaseLost
	}
	return nil
}

// Release, iÅŸ mantÄ±ÄŸÄ± hata ile baÅŸarÄ±sÄ±z olduÄŸunda kilidi serbest bÄ±rakÄ±r
// ki aynÄ± key ile yeniden deneme (retry) mÃ¼mkÃ¼n olsun. Sadece ownerToken
// hÃ¢lÃ¢ geÃ§erliyse (bu worker hÃ¢lÃ¢ kilidin sahibiyse) silme iÅŸlemi yapÄ±lÄ±r.
func (s *Store) Release(ctx context.Context, idempotencyKey, ownerToken string) error {
	k := s.key(idempotencyKey)
	res, err := releaseScript.Run(ctx, s.rdb, []string{k}, ownerToken).Result()
	if err != nil {
		return fmt.Errorf("redis release scripti baÅŸarÄ±sÄ±z: %w", err)
	}
	if n, _ := res.(int64); n != 1 {
		return ErrLeaseLost
	}
	return nil
}

// RenewLease, uzun sÃ¼ren iÅŸlemler iÃ§in in-flight kilidin TTL'sini uzatÄ±r.
// Ã‡aÄŸÄ±ran taraf (Ã¶rn. beklenenden uzun sÃ¼ren bir dÄ±ÅŸ saÄŸlayÄ±cÄ± Ã§aÄŸrÄ±sÄ±
// sÄ±rasÄ±nda) periyodik olarak bunu Ã§aÄŸÄ±rarak lease'in sÃ¼resinin dolup
// baÅŸka bir worker'Ä±n aynÄ± key'i "yeni" sanmasÄ±nÄ± engelleyebilir.
func (s *Store) RenewLease(ctx context.Context, idempotencyKey, ownerToken string) error {
	k := s.key(idempotencyKey)
	res, err := renewScript.Run(ctx, s.rdb, []string{k}, ownerToken, s.leaseTTL.Milliseconds()).Result()
	if err != nil {
		return fmt.Errorf("redis renew scripti baÅŸarÄ±sÄ±z: %w", err)
	}
	if n, _ := res.(int64); n != 1 {
		return ErrLeaseLost
	}
	return nil
}

// sanitizeForLua, gÃ¶mÃ¼lÃ¼ Lua script literallerine tek tÄ±rnak/backslash
// iÃ§eren deÄŸerlerin (bu pakette owner UUID/hash olduÄŸu iÃ§in pratikte hiÃ§
// olmaz) kaÃ§madan sÄ±zmasÄ±nÄ± Ã¶nlemek iÃ§in kullanÄ±labilecek bir yardÄ±mcÄ±dÄ±r.
// Åu an owner=uuid.NewString() ve req_hash=hex.EncodeToString(...) her
// zaman [0-9a-f-] karakter kÃ¼mesiyle sÄ±nÄ±rlÄ± olduÄŸu iÃ§in ekstra kaÃ§Ä±ÅŸa
// gerek yoktur; bu fonksiyon gelecekte farklÄ± bir owner/kimlik Ã¼retim
// stratejisine geÃ§ilirse savunma amaÃ§lÄ± burada tutulur.
func sanitizeForLua(s string) string {
	replacer := strings.NewReplacer(`\`, `\\`, `'`, `\'`)
	return replacer.Replace(s)
}