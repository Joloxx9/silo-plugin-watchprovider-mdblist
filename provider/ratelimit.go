package provider

import (
	"crypto/sha256"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// limiterIdleTTL is how long an API key's limiter may sit unused before it
// becomes eligible for removal.
const limiterIdleTTL = 10 * time.Minute

// keyLimiters paces requests per MDBList API key, so one throttled account
// never delays another account's sync. Limiters are keyed by a SHA-256 digest
// of the key: the map never holds a raw secret.
type keyLimiters struct {
	every time.Duration
	burst int
	now   func() time.Time

	mu        sync.Mutex
	limiters  map[[sha256.Size]byte]*limiterEntry
	lastSweep time.Time
}

type limiterEntry struct {
	limiter  *rate.Limiter
	lastUsed time.Time
}

func newKeyLimiters(every time.Duration, burst int) *keyLimiters {
	return &keyLimiters{
		every:    every,
		burst:    max(burst, 1),
		now:      time.Now,
		limiters: make(map[[sha256.Size]byte]*limiterEntry),
	}
}

func (l *keyLimiters) forKey(apiKey string) *rate.Limiter {
	key := sha256.Sum256([]byte(apiKey))
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	if now.Sub(l.lastSweep) >= limiterIdleTTL {
		l.lastSweep = now
		l.sweepLocked(now)
	}
	entry, ok := l.limiters[key]
	if !ok {
		entry = &limiterEntry{limiter: rate.NewLimiter(rate.Every(l.every), l.burst)}
		l.limiters[key] = entry
	}
	entry.lastUsed = now
	return entry.limiter
}

// sweepLocked drops idle limiters so the map stays bounded as keys change. An
// idle limiter with a full bucket behaves exactly like a new one, so removing
// it cannot let a key exceed its rate.
func (l *keyLimiters) sweepLocked(now time.Time) {
	for key, entry := range l.limiters {
		if now.Sub(entry.lastUsed) >= limiterIdleTTL && entry.limiter.TokensAt(now) >= float64(l.burst) {
			delete(l.limiters, key)
		}
	}
}
