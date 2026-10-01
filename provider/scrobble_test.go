package provider

import (
	"encoding/json"
	"net/http"
	"testing"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
)

const (
	scrobbleStart = pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SCROBBLE_START
	scrobblePause = pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SCROBBLE_PAUSE
	scrobbleStop  = pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SCROBBLE_STOP
)

func TestScrobbleStartUsesEpisodeShape(t *testing.T) {
	var gotPath string
	var gotBody map[string]any
	s := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusOK)
	})
	event := episodeEvent("start", scrobbleStart, nil, map[string]string{"imdb": "tt0903747"}, 3, 7)
	event.PositionSeconds, event.DurationSeconds = 600, 2400
	response := applyEvents(t, s, event)
	assertStatus(t, response, "start", statusApplied)
	if gotPath != "/scrobble/start" {
		t.Fatalf("path = %q, want /scrobble/start", gotPath)
	}
	if progress, _ := gotBody["progress"].(float64); progress != 25 {
		t.Fatalf("progress = %v, want 25", gotBody["progress"])
	}
	show, _ := gotBody["show"].(map[string]any)
	season, _ := show["season"].(map[string]any)
	if number, _ := season["number"].(float64); int(number) != 3 {
		t.Fatalf("show season number = %v, want 3", season["number"])
	}
	episode, _ := season["episode"].(map[string]any)
	if number, _ := episode["number"].(float64); int(number) != 7 {
		t.Fatalf("show episode number = %v, want 7", episode["number"])
	}
	if _, exists := gotBody["season"]; exists {
		t.Fatalf("season must be nested under show: %#v", gotBody)
	}
	if _, exists := show["episode"]; exists {
		t.Fatalf("episode must be nested under show season: %#v", show)
	}
	if ids, _ := show["ids"].(map[string]any); ids["imdb"] != "tt0903747" {
		t.Fatalf("show ids = %v, want imdb tt0903747", show["ids"])
	}
}

func TestScrobbleEpisodeFallsBackToItsOwnIDs(t *testing.T) {
	var gotBody map[string]any
	s := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
	})
	applyEvents(t, s, episodeEvent("start", scrobbleStart, map[string]string{"tvdb": "349232"}, nil, 1, 2))
	show, _ := gotBody["show"].(map[string]any)
	if ids, _ := show["ids"].(map[string]any); ids["tvdb"] != float64(349232) {
		t.Fatalf("show ids = %v, want the episode's own IDs", show["ids"])
	}
}

func TestScrobbleRoundsProgressToMDBListPrecision(t *testing.T) {
	var gotBody map[string]any
	s := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusCreated)
	})
	event := movieEvent("start", scrobbleStart, map[string]string{"tmdb": "950387"})
	event.PositionSeconds, event.DurationSeconds = 611.8, 2163.4
	applyEvents(t, s, event)
	if got := gotBody["progress"]; got != 28.28 {
		t.Fatalf("progress = %v, want 28.28", got)
	}
	movie, _ := gotBody["movie"].(map[string]any)
	if ids, _ := movie["ids"].(map[string]any); ids["tmdb"] != float64(950387) {
		t.Fatalf("movie ids = %v", movie["ids"])
	}
}

func TestScrobbleSendsEachOperationToItsEndpoint(t *testing.T) {
	var paths []string
	s := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		w.WriteHeader(http.StatusNoContent)
	})
	ids := map[string]string{"imdb": "tt0111161"}
	response := applyEvents(t, s,
		movieEvent("start", scrobbleStart, ids),
		movieEvent("pause", scrobblePause, ids),
		movieEvent("stop", scrobbleStop, ids),
	)
	for _, id := range []string{"start", "pause", "stop"} {
		assertStatus(t, response, id, statusApplied)
	}
	if len(paths) != 3 || paths[0] != "/scrobble/start" || paths[1] != "/scrobble/pause" || paths[2] != "/scrobble/stop" {
		t.Fatalf("paths = %v", paths)
	}
}

func TestScrobbleMapsUpstreamFailures(t *testing.T) {
	cases := map[int]pluginv1.WatchSyncApplyStatus{
		http.StatusBadRequest:          statusRejected,
		http.StatusInternalServerError: statusRetry,
	}
	for status, want := range cases {
		s := newTestServer(t, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`{"error":{"progress":["Ensure that there are no more than 5 digits in total."]}}`))
		})
		response := applyEvents(t, s, movieEvent("start", scrobbleStart, map[string]string{"imdb": "tt0111161"}))
		result := assertStatus(t, response, "start", want)
		if result.GetFault().GetSafeMessage() == "" {
			t.Fatalf("status %d: fault = %v, want a safe message", status, result.GetFault())
		}
	}
}

func TestScrobbleRejectsUnidentifiedTitleWithoutCallingMDBList(t *testing.T) {
	s := newTestServer(t, func(http.ResponseWriter, *http.Request) {
		t.Error("MDBList should not be called")
	})
	response := applyEvents(t, s,
		movieEvent("movie", scrobbleStart, nil),
		seriesEvent("series", scrobbleStart, map[string]string{"tvdb": "81189"}),
	)
	assertStatus(t, response, "movie", statusRejected)
	assertStatus(t, response, "series", statusRejected)
}
