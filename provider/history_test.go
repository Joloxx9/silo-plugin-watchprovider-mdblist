package provider

import (
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
)

const (
	markWatched   = pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_MARK_WATCHED
	markUnwatched = pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_MARK_UNWATCHED

	statusApplied  = pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED
	statusNoChange = pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_NO_CHANGE
	statusRetry    = pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_RETRY
	statusRejected = pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_REJECTED
)

// historyServer fakes MDBList's watched history: GET answers with history,
// and POST records the body and answers with writeResponse.
type historyServer struct {
	history       string
	writeResponse string
	reads         []string
	writes        []string
}

func (h *historyServer) handle(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			if r.URL.Path != "/sync/watched" {
				t.Errorf("GET %s, want /sync/watched", r.URL.Path)
			}
			h.reads = append(h.reads, r.URL.RawQuery)
			writeJSON(w, h.history)
		case http.MethodPost:
			body, _ := io.ReadAll(r.Body)
			h.writes = append(h.writes, r.URL.Path+" "+string(body))
			writeJSON(w, h.writeResponse)
		}
	}
}

func TestMarkWatchedSendsBulkPayload(t *testing.T) {
	fake := &historyServer{history: `{"movies":[],"episodes":[]}`, writeResponse: `{"added":{},"updated":{},"not_found":{}}`}
	s := newTestServer(t, fake.handle(t))
	response := applyEvents(t, s,
		at(movieEvent("h1", markWatched, map[string]string{"imdb": "tt0111161", "tmdb": "278"}), time.Date(2025, 10, 21, 12, 0, 0, 0, time.UTC)),
		at(episodeEvent("h2", markWatched, nil, map[string]string{"imdb": "tt0903747"}, 1, 2), time.Date(2025, 10, 22, 13, 0, 0, 0, time.UTC)),
	)
	assertStatus(t, response, "h1", statusApplied)
	assertStatus(t, response, "h2", statusApplied)
	if len(fake.writes) != 1 {
		t.Fatalf("writes = %v, want one bulk write", fake.writes)
	}
	path, body, _ := cutWrite(fake.writes[0])
	if path != "/sync/watched" {
		t.Fatalf("write path = %q", path)
	}
	assertJSONEqual(t, []byte(body), `{
		"movies":[{"ids":{"imdb":"tt0111161","tmdb":278},"watched_at":"2025-10-21T12:00:00Z"}],
		"episodes":[{"ids":{},"show":{"ids":{"imdb":"tt0903747"}},"season":1,"episode":2,"watched_at":"2025-10-22T13:00:00Z"}]
	}`)
}

func TestMarkWatchedSkipsPlaysMDBListAlreadyHolds(t *testing.T) {
	played := time.Date(2025, 10, 21, 12, 0, 0, 0, time.UTC)
	fake := &historyServer{
		history: `{
			"movies":[{"watched_at":"2025-10-21T12:00:00Z","movie":{"ids":{"imdb":"tt0111161","tmdb":278}}}],
			"episodes":[{"watched_at":"2025-10-22T13:00:00Z","episode":{"season":1,"number":2,"ids":{"tmdb":62086},"show":{"ids":{"tvdb":81189}}}}],
			"pagination":{"next_cursor":null}
		}`,
		writeResponse: `{"updated":{"movies":1}}`,
	}
	s := newTestServer(t, fake.handle(t))
	response := applyEvents(t, s,
		// The same play, sent again with sub-second precision.
		at(movieEvent("same", markWatched, map[string]string{"tmdb": "278"}), played.Add(400*time.Millisecond)),
		// An episode matched by its series and numbers rather than its own ID.
		at(episodeEvent("episode", markWatched, map[string]string{"tvdb": "349232"}, map[string]string{"tvdb": "81189"}, 1, 2), time.Date(2025, 10, 22, 13, 0, 0, 0, time.UTC)),
		// A rewatch of the same movie a second later is a new play.
		at(movieEvent("rewatch", markWatched, map[string]string{"imdb": "tt0111161"}), played.Add(time.Second)),
	)
	assertStatus(t, response, "same", statusNoChange)
	assertStatus(t, response, "episode", statusNoChange)
	assertStatus(t, response, "rewatch", statusApplied)
	if len(fake.reads) != 1 || len(fake.writes) != 1 {
		t.Fatalf("reads = %v writes = %v, want one history read and one write", fake.reads, fake.writes)
	}
	_, body, _ := cutWrite(fake.writes[0])
	assertJSONEqual(t, []byte(body), `{"movies":[{"ids":{"imdb":"tt0111161"},"watched_at":"2025-10-21T12:00:01Z"}]}`)
}

func TestMarkWatchedReadsHistorySinceTheEarliestPlay(t *testing.T) {
	fake := &historyServer{history: `{"movies":[],"pagination":{"next_cursor":null}}`, writeResponse: `{}`}
	s := newTestServer(t, fake.handle(t))
	applyEvents(t, s,
		at(movieEvent("late", markWatched, map[string]string{"imdb": "tt1"}), time.Date(2025, 10, 23, 0, 0, 0, 0, time.UTC)),
		at(movieEvent("early", markWatched, map[string]string{"imdb": "tt2"}), time.Date(2025, 10, 21, 12, 30, 0, 0, time.UTC)),
	)
	if len(fake.reads) != 1 {
		t.Fatalf("reads = %v", fake.reads)
	}
	query := parseQuery(t, fake.reads[0])
	if query.Get("plays") != "all" || query.Get("since") != "2025-10-20T12:30:00Z" || query.Get("limit") != "1000" {
		t.Fatalf("history read query = %q, want plays=all since a day before the earliest play", fake.reads[0])
	}
}

func TestMarkWatchedFollowsHistoryPages(t *testing.T) {
	var reads []string
	writes := 0
	s := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			writes++
			writeJSON(w, `{}`)
			return
		}
		reads = append(reads, r.URL.Query().Get("cursor"))
		if r.URL.Query().Get("plays") != "all" {
			t.Errorf("history read %q lacks plays=all", r.URL.RawQuery)
		}
		if r.URL.Query().Get("cursor") == "p2" {
			if r.URL.Query().Has("since") {
				t.Errorf("cursor page %q repeats since", r.URL.RawQuery)
			}
			writeJSON(w, `{"movies":[{"watched_at":"2025-10-21T12:00:00Z","movie":{"ids":{"imdb":"tt0111161"}}}],"pagination":{"next_cursor":null}}`)
			return
		}
		writeJSON(w, `{"movies":[],"pagination":{"next_cursor":"p2"}}`)
	})
	response := applyEvents(t, s, at(movieEvent("h1", markWatched, map[string]string{"imdb": "tt0111161"}), time.Date(2025, 10, 21, 12, 0, 0, 0, time.UTC)))
	assertStatus(t, response, "h1", statusNoChange)
	if len(reads) != 2 || writes != 0 {
		t.Fatalf("reads = %v writes = %d, want both history pages and no write", reads, writes)
	}
}

func TestMarkWatchedRetriesNotFoundBatch(t *testing.T) {
	fake := &historyServer{
		history:       `{"movies":[]}`,
		writeResponse: `{"added":{"movies":[]},"updated":{},"not_found":{"movies":[{"ids":{"imdb":"tt-missing"}}]}}`,
	}
	s := newTestServer(t, fake.handle(t))
	response := applyEvents(t, s,
		at(movieEvent("h1", markWatched, map[string]string{"imdb": "tt-missing"}), time.Now()),
		at(movieEvent("h2", markWatched, map[string]string{"imdb": "tt0111161"}), time.Now()),
	)
	for _, id := range []string{"h1", "h2"} {
		result := assertStatus(t, response, id, statusRetry)
		if result.GetFault().GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_TEMPORARY {
			t.Fatalf("%s fault = %v", id, result.GetFault())
		}
	}
}

func TestMarkWatchedTreatsEmptyNotFoundBucketsAsSuccess(t *testing.T) {
	fake := &historyServer{history: `{"movies":[]}`, writeResponse: `{"added":{"movies":1},"not_found":{"movies":[],"episodes":[]}}`}
	s := newTestServer(t, fake.handle(t))
	response := applyEvents(t, s, at(movieEvent("h1", markWatched, map[string]string{"imdb": "tt0111161"}), time.Now()))
	assertStatus(t, response, "h1", statusApplied)
}

func TestMarkWatchedRejectsUnidentifiedPlayWithoutCallingMDBList(t *testing.T) {
	s := newTestServer(t, func(http.ResponseWriter, *http.Request) {
		t.Error("MDBList should not be called for an unsupported play")
	})
	response := applyEvents(t, s,
		at(movieEvent("no-ids", markWatched, nil), time.Now()),
		at(seriesEvent("series", markWatched, map[string]string{"tvdb": "81189"}), time.Now()),
	)
	for _, id := range []string{"no-ids", "series"} {
		result := assertStatus(t, response, id, statusRejected)
		if result.GetFault().GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_REQUEST {
			t.Fatalf("%s fault = %v", id, result.GetFault())
		}
	}
}

func TestMarkWatchedStillWritesWhenHistoryReadFails(t *testing.T) {
	writes := 0
	s := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			writes++
			writeJSON(w, `{"updated":{"movies":1}}`)
			return
		}
		w.WriteHeader(http.StatusBadGateway)
	})
	response := applyEvents(t, s, at(movieEvent("h1", markWatched, map[string]string{"imdb": "tt0111161"}), time.Now()))
	assertStatus(t, response, "h1", statusApplied)
	if writes != 1 {
		t.Fatalf("writes = %d, want the play written despite the failed history read", writes)
	}
}

func TestMarkWatchedBoundsTheHistoryRead(t *testing.T) {
	reads, writes := 0, 0
	s := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			writes++
			writeJSON(w, `{}`)
			return
		}
		reads++
		writeJSON(w, fmt.Sprintf(`{"movies":[],"pagination":{"next_cursor":"page-%d"}}`, reads))
	})
	response := applyEvents(t, s, at(movieEvent("h1", markWatched, map[string]string{"imdb": "tt0111161"}), time.Now()))
	assertStatus(t, response, "h1", statusApplied)
	if reads != maxHistoryCheckPages || writes != 1 {
		t.Fatalf("reads = %d writes = %d, want %d history pages and one write", reads, writes, maxHistoryCheckPages)
	}
}

func TestMarkWatchedStopsOnARateLimitedHistoryRead(t *testing.T) {
	writes := 0
	s := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			writes++
		}
		w.WriteHeader(http.StatusTooManyRequests)
	})
	response := applyEvents(t, s, at(movieEvent("h1", markWatched, map[string]string{"imdb": "tt0111161"}), time.Now()))
	result := assertStatus(t, response, "h1", statusRetry)
	if result.GetFault().GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_RATE_LIMITED || writes != 0 {
		t.Fatalf("fault = %v writes = %d, want a rate-limit retry and no write", result.GetFault(), writes)
	}
}

func TestRejectedWriteRejectsASingleEventAndRetriesABatch(t *testing.T) {
	fake := &historyServer{history: `{}`}
	s := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			fake.handle(t)(w, r)
			return
		}
		w.WriteHeader(http.StatusBadRequest)
	})
	single := applyEvents(t, s, at(movieEvent("one", markWatched, map[string]string{"imdb": "tt1"}), time.Now()))
	if result := assertStatus(t, single, "one", statusRejected); result.GetFault().GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_REQUEST {
		t.Fatalf("fault = %v", result.GetFault())
	}
	batch := applyEvents(t, s,
		at(movieEvent("a", markWatched, map[string]string{"imdb": "tt1"}), time.Now()),
		at(movieEvent("b", markWatched, map[string]string{"imdb": "tt2"}), time.Now()),
	)
	assertStatus(t, batch, "a", statusRetry)
	assertStatus(t, batch, "b", statusRetry)
}

func TestMarkUnwatchedRemovesTitlesWithoutWatchTimes(t *testing.T) {
	fake := &historyServer{writeResponse: `{"removed":{"movies":1,"episodes":1}}`}
	s := newTestServer(t, fake.handle(t))
	response := applyEvents(t, s,
		at(movieEvent("u1", markUnwatched, map[string]string{"imdb": "tt0111161"}), time.Now()),
		at(episodeEvent("u2", markUnwatched, map[string]string{"tvdb": "349232"}, map[string]string{"tvdb": "81189"}, 1, 2), time.Now()),
	)
	assertStatus(t, response, "u1", statusApplied)
	assertStatus(t, response, "u2", statusApplied)
	if len(fake.reads) != 0 || len(fake.writes) != 1 {
		t.Fatalf("reads = %v writes = %v", fake.reads, fake.writes)
	}
	path, body, _ := cutWrite(fake.writes[0])
	if path != "/sync/watched/remove" {
		t.Fatalf("path = %q", path)
	}
	assertJSONEqual(t, []byte(body), `{
		"movies":[{"ids":{"imdb":"tt0111161"}}],
		"episodes":[{"ids":{"tvdb":349232},"show":{"ids":{"tvdb":81189}},"season":1,"episode":2}]
	}`)
}

func TestMarkUnwatchedTreatsNotFoundAsAlreadyUnwatched(t *testing.T) {
	fake := &historyServer{writeResponse: `{"removed":{"movies":0},"not_found":{"movies":1}}`}
	s := newTestServer(t, fake.handle(t))
	response := applyEvents(t, s, movieEvent("u1", markUnwatched, map[string]string{"imdb": "tt0111161"}))
	assertStatus(t, response, "u1", statusNoChange)
}
