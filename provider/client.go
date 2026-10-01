package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	"golang.org/x/time/rate"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	defaultBaseURL = "https://api.mdblist.com"

	// MDBList enforces a daily request quota (1,000 a day on the free tier) and
	// a five-minute window limit. Pacing each API key to about one request a
	// second keeps paginated reads and chunked writes under the window limit;
	// a 429 defers the whole sync instead.
	requestInterval = time.Second
	requestBurst    = 2

	// A 429 with a short Retry-After is retried in place so pagination
	// survives burst-limit blips; anything longer defers the sync run.
	maxInPlaceRetryWait = 15 * time.Second
	maxRetryAttempts    = 2

	// Without a usable Retry-After a burst limit and an exhausted daily quota
	// look the same, so back off until the next scheduled sync.
	defaultRetryAfter = time.Hour

	defaultRequestTimeout = 20 * time.Second
	maxResponseBytes      = 32 << 20
	redactedPlaceholder   = "[REDACTED]"
)

var errMissingAPIKey = errors.New("mdblist api key is missing")

// apiClient serves one RPC for one API key.
type apiClient struct {
	baseURL string
	apiKey  string
	http    *http.Client
	limiter *rate.Limiter
	sleep   func(context.Context, time.Duration) error
}

func (c *apiClient) get(ctx context.Context, path string, query url.Values, out any) error {
	return c.do(ctx, http.MethodGet, path, query, nil, out)
}

func (c *apiClient) post(ctx context.Context, path string, payload, out any) error {
	return c.do(ctx, http.MethodPost, path, nil, payload, out)
}

// do sends one request, pacing it per API key and retrying a short rate limit
// in place. A longer or repeated rate limit returns a *rateLimitedError.
func (c *apiClient) do(ctx context.Context, method, path string, query url.Values, payload, out any) error {
	if strings.TrimSpace(c.apiKey) == "" {
		return errMissingAPIKey
	}
	var body []byte
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("encode mdblist request: %w", err)
		}
		body = encoded
	}
	for attempt := 0; ; attempt++ {
		if err := c.limiter.Wait(ctx); err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			// The next slot lies past the call's deadline: the request was
			// never sent, so leave the work for a later run.
			return &rateLimitedError{retryAfter: requestInterval}
		}
		err := c.once(ctx, method, path, query, body, out)
		var limited *rateLimitedError
		if !errors.As(err, &limited) {
			return err
		}
		wait := limited.retryAfter
		if wait <= 0 {
			wait = defaultRetryAfter
		}
		if attempt >= maxRetryAttempts || wait > maxInPlaceRetryWait || !fitsDeadline(ctx, wait) {
			// Exhausted in-place retries mean the short Retry-After hints were
			// not trustworthy; floor the deferral so the sync does not come
			// straight back into the same limit.
			if attempt >= maxRetryAttempts && wait < defaultRetryAfter {
				wait = defaultRetryAfter
			}
			return &rateLimitedError{retryAfter: wait}
		}
		if err := c.sleep(ctx, wait); err != nil {
			return err
		}
	}
}

// once performs a single HTTP attempt. The request URL carries the API key,
// so it never leaves this function: errors name the request by method and
// path, and transport errors are masked by requestError.
func (c *apiClient) once(ctx context.Context, method, path string, query url.Values, payload []byte, out any) error {
	var body io.Reader
	if payload != nil {
		body = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.requestURL(path, query), body)
	if err != nil {
		return requestError("create", c.apiKey, err)
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return requestError("send", c.apiKey, err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxResponseBytes))
		_ = resp.Body.Close()
	}()

	if resp.StatusCode == http.StatusTooManyRequests {
		// An absent, malformed, or elapsed Retry-After yields 0, which do
		// replaces with defaultRetryAfter.
		wait, _ := parseRetryAfter(resp.Header.Get("Retry-After"), time.Now())
		return &rateLimitedError{retryAfter: wait}
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return &statusError{method: method, path: describePath(path, query), status: resp.StatusCode}
	}
	if out == nil || resp.StatusCode == http.StatusNoContent {
		return nil
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxResponseBytes)).Decode(out); err != nil {
		return &decodeError{cause: err}
	}
	return nil
}

// requestURL appends the API key MDBList expects as a query parameter. The
// result is a credential and must never reach error text.
func (c *apiClient) requestURL(path string, query url.Values) string {
	encoded := query.Encode()
	if encoded != "" {
		encoded += "&"
	}
	return c.baseURL + path + "?" + encoded + "apikey=" + url.QueryEscape(c.apiKey)
}

func describePath(path string, query url.Values) string {
	if encoded := query.Encode(); encoded != "" {
		return path + "?" + encoded
	}
	return path
}

// fitsDeadline reports whether a wait still leaves the call time to send the
// request again.
func fitsDeadline(ctx context.Context, wait time.Duration) bool {
	deadline, ok := ctx.Deadline()
	return !ok || time.Until(deadline) > wait+time.Second
}

func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// parseRetryAfter reads an RFC 9110 Retry-After value: delay-seconds or an
// HTTP-date. ok is false when the value is absent or malformed. An HTTP-date
// that has already passed parses as zero.
func parseRetryAfter(value string, now time.Time) (time.Duration, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false
	}
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil {
		if seconds < 0 {
			return 0, false
		}
		if seconds > int64(math.MaxInt64/time.Second) {
			return time.Duration(math.MaxInt64), true
		}
		return time.Duration(seconds) * time.Second, true
	}
	if at, err := http.ParseTime(value); err == nil {
		return max(at.Sub(now), 0), true
	}
	return 0, false
}

type rateLimitedError struct {
	retryAfter time.Duration
}

func (e *rateLimitedError) Error() string {
	return fmt.Sprintf("mdblist rate limit reached; retry after %s", e.retryAfter)
}

type statusError struct {
	method string
	path   string
	status int
}

func (e *statusError) Error() string {
	return fmt.Sprintf("mdblist request %s %s failed: status %d", e.method, e.path, e.status)
}

type decodeError struct {
	cause error
}

func (e *decodeError) Error() string { return "decode mdblist response: " + e.cause.Error() }
func (e *decodeError) Unwrap() error { return e.cause }

// faultFor maps a request error to the fault Silo acts on. Safe messages are
// fixed text: they never carry the API key or an upstream response body.
func faultFor(err error) *pluginv1.WatchSyncFault {
	var limited *rateLimitedError
	var status *statusError
	var decode *decodeError
	switch {
	case err == nil:
		return nil
	case errors.As(err, &limited):
		return &pluginv1.WatchSyncFault{
			Code:        pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_RATE_LIMITED,
			SafeMessage: "MDBList rate limit reached",
			RetryAfter:  durationpb.New(limited.retryAfter),
		}
	case errors.Is(err, errMissingAPIKey):
		return invalidCredentialFault("MDBList API key is missing")
	case errors.As(err, &status):
		return statusFault(status.status)
	case errors.As(err, &decode):
		return temporaryFault("MDBList returned an unreadable response")
	default:
		return temporaryFault("MDBList is temporarily unreachable")
	}
}

func statusFault(status int) *pluginv1.WatchSyncFault {
	switch {
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		return invalidCredentialFault("MDBList rejected the API key")
	case status == http.StatusRequestTimeout || status >= http.StatusInternalServerError:
		return temporaryFault(fmt.Sprintf("MDBList is temporarily unavailable (HTTP %d)", status))
	case status == http.StatusBadRequest || status == http.StatusNotFound ||
		status == http.StatusConflict || status == http.StatusUnprocessableEntity:
		return invalidRequestFault(fmt.Sprintf("MDBList rejected the request (HTTP %d)", status))
	default:
		return permanentFault(fmt.Sprintf("MDBList request failed (HTTP %d)", status))
	}
}

// requestError reports a failure to build or send a request without the API
// key. http.NewRequestWithContext and http.Client.Do return a *url.Error whose
// message embeds the request URL, key included, and a redirect with an
// unparseable Location quotes that server-supplied value too.
func requestError(stage, apiKey string, err error) error {
	if msg := err.Error(); redactAPIKey(msg, apiKey) != msg {
		err = redactedError{message: redactAPIKey(msg, apiKey), cause: err}
	}
	return fmt.Errorf("%s mdblist request: %w", stage, err)
}

// redactedError carries a message with the API key masked. It answers
// errors.Is and errors.As from the original error, so cancellations and
// timeouts stay classifiable, but has no Unwrap: walking the chain never
// reaches the original, key-bearing message.
type redactedError struct {
	message string
	cause   error
}

func (e redactedError) Error() string        { return e.message }
func (e redactedError) Is(target error) bool { return errors.Is(e.cause, target) }
func (e redactedError) As(target any) bool   { return errors.As(e.cause, target) }

// redactAPIKey masks every occurrence of the API key, raw or query-escaped.
func redactAPIKey(text, apiKey string) string {
	if apiKey == "" {
		return text
	}
	text = strings.ReplaceAll(text, apiKey, redactedPlaceholder)
	if escaped := url.QueryEscape(apiKey); escaped != apiKey {
		text = strings.ReplaceAll(text, escaped, redactedPlaceholder)
	}
	return text
}
