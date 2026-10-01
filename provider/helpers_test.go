package provider

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const testKey = "test-key"

// newTestServer returns a Server that talks to handler without pacing or
// in-place retry waits.
func newTestServer(t *testing.T, handler http.HandlerFunc) *Server {
	t.Helper()
	upstream := httptest.NewServer(handler)
	t.Cleanup(upstream.Close)
	return testServerFor(upstream.Client(), upstream.URL)
}

func testServerFor(client *http.Client, baseURL string) *Server {
	s := NewServer(client)
	s.baseURL = baseURL
	s.limiters = newKeyLimiters(time.Microsecond, 1000)
	s.sleep = func(ctx context.Context, _ time.Duration) error { return ctx.Err() }
	return s
}

func authContext(apiKey string) *pluginv1.WatchSyncAuthenticatedContext {
	return &pluginv1.WatchSyncAuthenticatedContext{
		CapabilityId: capabilityID,
		Credentials:  &pluginv1.WatchSyncCredentials{AccessToken: apiKey},
	}
}

func writeJSON(w http.ResponseWriter, body string) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(body))
}

// traversal is every page of one ListRemoteState traversal.
type traversal struct {
	items    []*pluginv1.WatchSyncRemoteState
	complete bool
	pages    int
	fault    *pluginv1.WatchSyncFault
	warnings []string
}

// listAll follows page tokens to the end of a traversal, or to its first
// fault, and checks the snapshot mode stays stable across pages.
func listAll(t *testing.T, s *Server, kind pluginv1.WatchSyncRemoteStateKind) traversal {
	t.Helper()
	return listAllWithKey(t, s, kind, testKey)
}

func listAllWithKey(t *testing.T, s *Server, kind pluginv1.WatchSyncRemoteStateKind, apiKey string) traversal {
	t.Helper()
	var result traversal
	token := ""
	seenTokens := map[string]bool{}
	for {
		response, err := s.ListRemoteState(context.Background(), &pluginv1.WatchSyncListRemoteStateRequest{
			Context:    authContext(apiKey),
			PageToken:  token,
			PageSize:   100,
			StateKinds: []pluginv1.WatchSyncRemoteStateKind{kind},
		})
		if err != nil {
			t.Fatalf("list remote state: %v", err)
		}
		result.pages++
		if response.GetFault() != nil {
			result.fault = response.GetFault()
			return result
		}
		if result.pages > 1 && response.GetCompleteSnapshot() != result.complete {
			t.Fatalf("page %d changed complete_snapshot to %t", result.pages, response.GetCompleteSnapshot())
		}
		result.complete = response.GetCompleteSnapshot()
		result.items = append(result.items, response.GetItems()...)
		result.warnings = append(result.warnings, response.GetWarnings()...)
		token = response.GetNextPageToken()
		if token == "" {
			if response.GetNextCursor() != "" {
				t.Fatalf("traversal returned a durable cursor %q", response.GetNextCursor())
			}
			return result
		}
		if seenTokens[token] {
			t.Fatalf("page token repeated: %q", token)
		}
		seenTokens[token] = true
	}
}

func applyEvents(t *testing.T, s *Server, events ...*pluginv1.WatchSyncEvent) *pluginv1.WatchSyncApplyEventsResponse {
	t.Helper()
	response, err := s.ApplyEvents(context.Background(), &pluginv1.WatchSyncApplyEventsRequest{
		Context: authContext(testKey),
		Events:  events,
	})
	if err != nil {
		t.Fatalf("apply events: %v", err)
	}
	if len(response.GetResults()) != 0 && len(response.GetResults()) != len(events) {
		t.Fatalf("got %d results for %d events", len(response.GetResults()), len(events))
	}
	return response
}

func resultFor(t *testing.T, response *pluginv1.WatchSyncApplyEventsResponse, eventID string) *pluginv1.WatchSyncApplyResult {
	t.Helper()
	for _, result := range response.GetResults() {
		if result.GetEventId() == eventID {
			return result
		}
	}
	t.Fatalf("no result for event %q in %v", eventID, response)
	return nil
}

func assertStatus(t *testing.T, response *pluginv1.WatchSyncApplyEventsResponse, eventID string, want pluginv1.WatchSyncApplyStatus) *pluginv1.WatchSyncApplyResult {
	t.Helper()
	result := resultFor(t, response, eventID)
	if result.GetStatus() != want {
		t.Fatalf("event %s status = %v (%v), want %v", eventID, result.GetStatus(), result.GetFault(), want)
	}
	return result
}

func movieEvent(id string, operation pluginv1.WatchSyncOperation, ids map[string]string) *pluginv1.WatchSyncEvent {
	return &pluginv1.WatchSyncEvent{
		EventId:   id,
		Operation: operation,
		Media: &pluginv1.WatchSyncMedia{
			MediaType:   pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_MOVIE,
			ExternalIds: ids,
		},
	}
}

func seriesEvent(id string, operation pluginv1.WatchSyncOperation, ids map[string]string) *pluginv1.WatchSyncEvent {
	event := movieEvent(id, operation, ids)
	event.Media.MediaType = pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_SERIES
	return event
}

func episodeEvent(id string, operation pluginv1.WatchSyncOperation, ids, seriesIDs map[string]string, season, episode int32) *pluginv1.WatchSyncEvent {
	return &pluginv1.WatchSyncEvent{
		EventId:   id,
		Operation: operation,
		Media: &pluginv1.WatchSyncMedia{
			MediaType:         pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE,
			ExternalIds:       ids,
			SeriesExternalIds: seriesIDs,
			SeasonNumber:      season,
			EpisodeNumber:     episode,
		},
	}
}

func at(event *pluginv1.WatchSyncEvent, when time.Time) *pluginv1.WatchSyncEvent {
	event.OccurredAt = timestamppb.New(when)
	return event
}

func assertJSONEqual(t *testing.T, got []byte, want string) {
	t.Helper()
	var gotValue, wantValue any
	if err := json.Unmarshal(got, &gotValue); err != nil {
		t.Fatalf("decode request body %q: %v", got, err)
	}
	if err := json.Unmarshal([]byte(want), &wantValue); err != nil {
		t.Fatalf("decode expected body: %v", err)
	}
	if !reflect.DeepEqual(gotValue, wantValue) {
		t.Fatalf("request body = %s, want %s", got, want)
	}
}

// cutWrite splits a recorded "path body" write.
func cutWrite(write string) (path, body string, ok bool) {
	return strings.Cut(write, " ")
}

func parseQuery(t *testing.T, raw string) url.Values {
	t.Helper()
	query, err := url.ParseQuery(raw)
	if err != nil {
		t.Fatalf("parse query %q: %v", raw, err)
	}
	return query
}
