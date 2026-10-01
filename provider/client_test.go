package provider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	"golang.org/x/time/rate"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
)

func testClient(s *Server) *apiClient {
	return s.client(testKey)
}

func TestShortRateLimitIsRetriedInPlace(t *testing.T) {
	attempts := 0
	s := newTestServer(t, func(w http.ResponseWriter, _ *http.Request) {
		attempts++
		if attempts == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		writeJSON(w, `{"user_id":7,"username":"retry"}`)
	})
	var waits []time.Duration
	s.sleep = func(_ context.Context, d time.Duration) error {
		waits = append(waits, d)
		return nil
	}
	response, _ := s.GetAccount(context.Background(), &pluginv1.WatchSyncGetAccountRequest{Context: authContext(testKey)})
	if response.GetFault() != nil || response.GetAccount().GetExternalSubject() != "7" {
		t.Fatalf("response = %v", response)
	}
	if attempts != 2 || len(waits) != 1 || waits[0] != time.Second {
		t.Fatalf("attempts = %d waits = %v, want one in-place retry after 1s", attempts, waits)
	}
}

func TestRateLimitFaults(t *testing.T) {
	cases := map[string]struct {
		retryAfter   func() string
		wantAttempts int
		check        func(time.Duration) bool
	}{
		// No Retry-After means a burst limit and an exhausted daily quota
		// look the same, so the provider defers a full hour.
		"without Retry-After": {func() string { return "" }, 1, func(d time.Duration) bool { return d == defaultRetryAfter }},
		"long Retry-After":    {func() string { return "3600" }, 1, func(d time.Duration) bool { return d == time.Hour }},
		// HTTP-dates have one-second resolution, so allow for truncation.
		"HTTP-date Retry-After": {
			func() string { return time.Now().Add(2 * time.Hour).UTC().Format(http.TimeFormat) }, 1,
			func(d time.Duration) bool { return d > 2*time.Hour-5*time.Second && d <= 2*time.Hour },
		},
		// The short hints proved untrustworthy, so the deferral is floored
		// rather than parroting the last 1s hint.
		"retries exhausted": {func() string { return "1" }, maxRetryAttempts + 1, func(d time.Duration) bool { return d == defaultRetryAfter }},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			attempts := 0
			s := newTestServer(t, func(w http.ResponseWriter, _ *http.Request) {
				attempts++
				if value := tc.retryAfter(); value != "" {
					w.Header().Set("Retry-After", value)
				}
				w.WriteHeader(http.StatusTooManyRequests)
			})
			s.sleep = func(context.Context, time.Duration) error { return nil }
			response, _ := s.GetAccount(context.Background(), &pluginv1.WatchSyncGetAccountRequest{Context: authContext(testKey)})
			fault := response.GetFault()
			if fault.GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_RATE_LIMITED || !tc.check(fault.GetRetryAfter().AsDuration()) {
				t.Fatalf("fault = %v", fault)
			}
			if attempts != tc.wantAttempts {
				t.Fatalf("attempts = %d, want %d", attempts, tc.wantAttempts)
			}
		})
	}
}

func TestRateLimitWaitThatOutlastsTheDeadlineIsDeferred(t *testing.T) {
	attempts := 0
	s := newTestServer(t, func(w http.ResponseWriter, _ *http.Request) {
		attempts++
		w.Header().Set("Retry-After", "10")
		w.WriteHeader(http.StatusTooManyRequests)
	})
	s.sleep = func(context.Context, time.Duration) error {
		t.Error("a wait past the deadline should not be slept")
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := testClient(s).get(ctx, "/user", nil, nil)
	var limited *rateLimitedError
	if !errors.As(err, &limited) || limited.retryAfter != 10*time.Second || attempts != 1 {
		t.Fatalf("err = %v attempts = %d, want a 10s rate limit without retrying", err, attempts)
	}
}

func TestRateLimitRetryReplaysBody(t *testing.T) {
	var bodies []string
	s := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		bodies = append(bodies, string(raw))
		if len(bodies) == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	if err := testClient(s).post(context.Background(), "/watchlist/items/add", mdblistListPayload{Movies: []mdblistRef{}}, nil); err != nil {
		t.Fatalf("post: %v", err)
	}
	if len(bodies) != 2 || bodies[0] != bodies[1] || bodies[0] != `{}` {
		t.Fatalf("body not replayed identically: %#v", bodies)
	}
}

func TestLimiterPacesEachAPIKeySeparately(t *testing.T) {
	s := NewServer(nil)
	first := s.limiters.forKey("a")
	if first != s.limiters.forKey("a") {
		t.Fatal("the same key should reuse one limiter")
	}
	if s.limiters.forKey("b") == first {
		t.Fatal("distinct keys should not share a limiter")
	}
	if first.Limit() != rate.Every(requestInterval) || first.Burst() != requestBurst {
		t.Fatalf("limiter = %v/%d, want one request a second with a burst of two", first.Limit(), first.Burst())
	}
	// The map is keyed by SHA-256 digests, so it never holds a raw key.
	if len(s.limiters.limiters) != 2 {
		t.Fatalf("limiters = %d, want one per key", len(s.limiters.limiters))
	}
}

func TestLimiterSweepDropsIdleKeys(t *testing.T) {
	limiters := newKeyLimiters(time.Second, 2)
	now := time.Now()
	limiters.now = func() time.Time { return now }
	limiters.forKey("idle")
	now = now.Add(limiterIdleTTL + time.Minute)
	limiters.forKey("active")
	if len(limiters.limiters) != 1 {
		t.Fatalf("limiters = %d, want the idle key dropped", len(limiters.limiters))
	}
}

func TestUpstreamFailuresMapToFaults(t *testing.T) {
	cases := map[int]pluginv1.WatchSyncFaultCode{
		http.StatusUnauthorized:        pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_CREDENTIAL,
		http.StatusForbidden:           pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_CREDENTIAL,
		http.StatusBadRequest:          pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_REQUEST,
		http.StatusNotFound:            pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_REQUEST,
		http.StatusRequestTimeout:      pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_TEMPORARY,
		http.StatusInternalServerError: pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_TEMPORARY,
		http.StatusBadGateway:          pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_TEMPORARY,
		http.StatusGone:                pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMANENT,
	}
	for status, want := range cases {
		s := newTestServer(t, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(status)
		})
		result := listAll(t, s, kindWatched)
		if result.fault.GetCode() != want {
			t.Errorf("status %d: fault = %v, want %v", status, result.fault, want)
		}
	}
	s := newTestServer(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, `{"movies":`)
	})
	if result := listAll(t, s, kindWatched); result.fault.GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_TEMPORARY {
		t.Errorf("unreadable response fault = %v, want TEMPORARY", result.fault)
	}
}

const sentinelAPIKey = "SENTINEL-KEY-123"

var errInjectedTransport = errors.New("injected transport failure")

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// assertErrorOmitsAPIKey fails when the key appears anywhere in err's chain,
// not only in its top-level message.
func assertErrorOmitsAPIKey(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected an error")
	}
	for e := err; e != nil; e = errors.Unwrap(e) {
		if strings.Contains(e.Error(), sentinelAPIKey) {
			t.Fatalf("error chain leaks the API key: %q", e.Error())
		}
	}
}

// assertResponseOmitsAPIKey fails when the key appears anywhere in a response.
func assertResponseOmitsAPIKey(t *testing.T, response proto.Message) {
	t.Helper()
	if text := prototext.Format(response); strings.Contains(text, sentinelAPIKey) {
		t.Fatalf("response leaks the API key: %s", text)
	}
}

// keyedRequests covers each request shape the plugin sends with the API key:
// GET with and without a query string, and POST with a JSON body. Each returns
// the response and the fault Silo would act on.
var keyedRequests = []struct {
	name string
	call func(context.Context, *Server) (proto.Message, *pluginv1.WatchSyncFault)
}{
	{"GET user", func(ctx context.Context, s *Server) (proto.Message, *pluginv1.WatchSyncFault) {
		response, _ := s.ExchangeAPIKey(ctx, &pluginv1.WatchSyncExchangeAPIKeyRequest{CapabilityId: capabilityID, ApiKey: sentinelAPIKey})
		return response, response.GetFault()
	}},
	{"GET watched page", func(ctx context.Context, s *Server) (proto.Message, *pluginv1.WatchSyncFault) {
		response, _ := s.ListRemoteState(ctx, &pluginv1.WatchSyncListRemoteStateRequest{
			Context: authContext(sentinelAPIKey), StateKinds: []pluginv1.WatchSyncRemoteStateKind{kindWatched},
		})
		return response, response.GetFault()
	}},
	{"GET playback", func(ctx context.Context, s *Server) (proto.Message, *pluginv1.WatchSyncFault) {
		response, _ := s.ListRemoteState(ctx, &pluginv1.WatchSyncListRemoteStateRequest{
			Context: authContext(sentinelAPIKey), StateKinds: []pluginv1.WatchSyncRemoteStateKind{kindProgress},
		})
		return response, response.GetFault()
	}},
	{"POST watched", func(ctx context.Context, s *Server) (proto.Message, *pluginv1.WatchSyncFault) {
		return applyWithKey(ctx, s, movieEvent("h1", markWatched, map[string]string{"imdb": "tt0111161"}))
	}},
	{"POST watchlist", func(ctx context.Context, s *Server) (proto.Message, *pluginv1.WatchSyncFault) {
		return applyWithKey(ctx, s, movieEvent("m1", addToWatchlist, map[string]string{"imdb": "tt0111161"}))
	}},
	{"POST scrobble", func(ctx context.Context, s *Server) (proto.Message, *pluginv1.WatchSyncFault) {
		event := movieEvent("s1", scrobbleStart, map[string]string{"imdb": "tt0111161"})
		event.PositionSeconds, event.DurationSeconds = 60, 600
		return applyWithKey(ctx, s, event)
	}},
}

// applyWithKey applies one event and returns the fault Silo would act on: the
// response's, or else the event's.
func applyWithKey(ctx context.Context, s *Server, event *pluginv1.WatchSyncEvent) (proto.Message, *pluginv1.WatchSyncFault) {
	response, _ := s.ApplyEvents(ctx, &pluginv1.WatchSyncApplyEventsRequest{
		Context: authContext(sentinelAPIKey),
		Events:  []*pluginv1.WatchSyncEvent{event},
	})
	if response.GetFault() != nil {
		return response, response.GetFault()
	}
	for _, result := range response.GetResults() {
		if result.GetFault() != nil {
			return response, result.GetFault()
		}
	}
	return response, nil
}

func TestRequestFailuresOmitAPIKey(t *testing.T) {
	sources := []struct {
		name      string
		newServer func(t *testing.T) *Server
		check     func(t *testing.T, err error)
	}{
		{
			name: "transport error",
			newServer: func(*testing.T) *Server {
				return testServerFor(&http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
					return nil, errInjectedTransport
				})}, "http://mdblist.test")
			},
			check: func(t *testing.T, err error) {
				if !errors.Is(err, errInjectedTransport) {
					t.Fatalf("lost the transport cause: %v", err)
				}
			},
		},
		{
			name: "connection refused",
			newServer: func(*testing.T) *Server {
				upstream := httptest.NewServer(http.NotFoundHandler())
				upstream.Close()
				return testServerFor(&http.Client{}, upstream.URL)
			},
			check: func(t *testing.T, err error) {
				var opErr *net.OpError
				if !errors.As(err, &opErr) {
					t.Fatalf("lost the dial error: %v", err)
				}
			},
		},
		{
			name: "malformed base URL",
			newServer: func(*testing.T) *Server {
				return testServerFor(&http.Client{}, "http://mdblist.test/\x7f")
			},
		},
		{
			name: "redirect loop",
			newServer: func(t *testing.T) *Server {
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					http.Redirect(w, r, r.URL.RequestURI(), http.StatusFound)
				}))
				t.Cleanup(upstream.Close)
				return testServerFor(upstream.Client(), upstream.URL)
			},
		},
		{
			name: "unparseable redirect",
			newServer: func(t *testing.T) *Server {
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Location", "http://%zz/?apikey="+r.URL.Query().Get("apikey"))
					w.WriteHeader(http.StatusFound)
				}))
				t.Cleanup(upstream.Close)
				return testServerFor(upstream.Client(), upstream.URL)
			},
		},
		{
			name: "error page echoing the URL",
			newServer: func(t *testing.T) *Server {
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusInternalServerError)
					_ = json.NewEncoder(w).Encode(map[string]string{"error": "unexpected failure for " + r.URL.RequestURI()})
				}))
				t.Cleanup(upstream.Close)
				return testServerFor(upstream.Client(), upstream.URL)
			},
		},
	}
	for _, source := range sources {
		for _, request := range keyedRequests {
			t.Run(source.name+"/"+request.name, func(t *testing.T) {
				response, fault := request.call(context.Background(), source.newServer(t))
				if fault == nil {
					t.Fatalf("response = %v, want a fault", response)
				}
				assertResponseOmitsAPIKey(t, response)
				if strings.Contains(fault.GetSafeMessage(), "unexpected failure") {
					t.Fatalf("fault quotes the upstream response: %q", fault.GetSafeMessage())
				}
			})
		}
		t.Run(source.name+"/error chain", func(t *testing.T) {
			s := source.newServer(t)
			err := s.client(sentinelAPIKey).get(context.Background(), "/sync/watched", url.Values{"limit": {"1000"}}, nil)
			assertErrorOmitsAPIKey(t, err)
			if source.check != nil {
				source.check(t, err)
			}
		})
	}
}

func TestRateLimitRetryFailureOmitsAPIKey(t *testing.T) {
	attempts := 0
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		attempts++
		if attempts == 1 {
			return &http.Response{
				StatusCode: http.StatusTooManyRequests,
				Header:     http.Header{"Retry-After": {"1"}},
				Body:       io.NopCloser(strings.NewReader("")),
				Request:    r,
			}, nil
		}
		return nil, errInjectedTransport
	})}
	s := testServerFor(client, "http://mdblist.test")
	err := s.client(sentinelAPIKey).post(context.Background(), "/watchlist/items/add", mdblistListPayload{}, nil)
	assertErrorOmitsAPIKey(t, err)
	if attempts != 2 || !errors.Is(err, errInjectedTransport) {
		t.Fatalf("attempts = %d err = %v, want the in-place retry to run and keep its cause", attempts, err)
	}
}

func TestCanceledRequestStillMatchesContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	release := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		cancel()
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer upstream.Close()
	defer close(release)

	err := testServerFor(upstream.Client(), upstream.URL).client(sentinelAPIKey).get(ctx, "/user", nil, nil)
	assertErrorOmitsAPIKey(t, err)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if faultFor(err).GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_TEMPORARY {
		t.Fatalf("fault = %v, want TEMPORARY", faultFor(err))
	}
}

func TestTimedOutRequestStaysDetectable(t *testing.T) {
	release := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer upstream.Close()
	defer close(release)

	cases := []struct {
		name   string
		client *http.Client
		ctx    func() (context.Context, context.CancelFunc)
	}{
		{
			name:   "client timeout",
			client: &http.Client{Timeout: 50 * time.Millisecond},
			ctx:    func() (context.Context, context.CancelFunc) { return context.WithCancel(context.Background()) },
		},
		{
			name:   "context deadline",
			client: &http.Client{},
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), 50*time.Millisecond)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := tc.ctx()
			defer cancel()
			err := testServerFor(tc.client, upstream.URL).client(sentinelAPIKey).get(ctx, "/user", nil, nil)
			assertErrorOmitsAPIKey(t, err)
			var netErr net.Error
			if !errors.As(err, &netErr) || !netErr.Timeout() {
				t.Fatalf("err = %v, want a net.Error timeout", err)
			}
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("err = %v, want context.DeadlineExceeded", err)
			}
		})
	}
}

func TestRedactAPIKeyMasksRawAndEscapedForms(t *testing.T) {
	const key = "k/y+z 1"
	text := "raw=" + key + " escaped=" + url.QueryEscape(key)
	if got, want := redactAPIKey(text, key), "raw="+redactedPlaceholder+" escaped="+redactedPlaceholder; got != want {
		t.Fatalf("redactAPIKey = %q, want %q", got, want)
	}
	if redactAPIKey("unchanged", "") != "unchanged" {
		t.Fatal("an empty key must not alter text")
	}
}

func TestRequestErrorKeepsCauseClassificationWhenMaskingTheMessage(t *testing.T) {
	cause := fmt.Errorf("GET https://api.mdblist.com/x?apikey=%s: %w", sentinelAPIKey, context.DeadlineExceeded)
	err := requestError("send", sentinelAPIKey, cause)
	assertErrorOmitsAPIKey(t, err)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want it to still match context.DeadlineExceeded", err)
	}
	var netErr net.Error
	timeout := &net.OpError{Op: "dial", Err: timeoutError{}}
	if !errors.As(requestError("send", sentinelAPIKey, fmt.Errorf("%s: %w", sentinelAPIKey, timeout)), &netErr) || !netErr.Timeout() {
		t.Fatal("a masked timeout must still be found as a net.Error")
	}
}

type timeoutError struct{}

func (timeoutError) Error() string   { return "i/o timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }
