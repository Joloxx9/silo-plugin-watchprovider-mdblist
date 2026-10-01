package provider

import (
	"io"
	"net/http"
	"testing"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
)

const (
	addToWatchlist      = pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_ADD_TO_WATCHLIST
	removeFromWatchlist = pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_REMOVE_FROM_WATCHLIST
)

func TestAddToWatchlistSendsMoviesAndShows(t *testing.T) {
	var gotPath string
	var gotBody []byte
	s := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotBody, _ = io.ReadAll(r.Body)
		writeJSON(w, `{"added":{"movies":1,"shows":1},"existing":{"movies":0,"shows":0},"not_found":{"movies":0,"shows":0}}`)
	})
	movie := movieEvent("m1", addToWatchlist, map[string]string{"imdb": "tt0111161"})
	position := int32(0)
	movie.ListPosition = &position
	show := seriesEvent("s1", addToWatchlist, nil)
	// A title known only by the key a previous read returned.
	show.ProviderItemKey = "tvdb:81189"
	response := applyEvents(t, s, movie, show)
	assertStatus(t, response, "m1", statusApplied)
	assertStatus(t, response, "s1", statusApplied)
	if gotPath != "/watchlist/items/add" {
		t.Fatalf("path = %q", gotPath)
	}
	assertJSONEqual(t, gotBody, `{"movies":[{"ids":{"imdb":"tt0111161"}}],"shows":[{"ids":{"tvdb":81189}}]}`)
}

func TestAddToWatchlistRetriesNotFoundBatch(t *testing.T) {
	s := newTestServer(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, `{"added":{"movies":0,"shows":0},"existing":{"movies":0,"shows":0},"not_found":{"movies":1,"shows":0}}`)
	})
	response := applyEvents(t, s, movieEvent("m1", addToWatchlist, map[string]string{"imdb": "tt0111161"}))
	result := assertStatus(t, response, "m1", statusRetry)
	if result.GetFault().GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_TEMPORARY {
		t.Fatalf("fault = %v", result.GetFault())
	}
}

func TestRemoveFromWatchlistTreatsNotFoundBatchAsReconciled(t *testing.T) {
	s := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/watchlist/items/remove" {
			t.Errorf("path = %q, want /watchlist/items/remove", r.URL.Path)
		}
		writeJSON(w, `{"removed":{"movies":0,"shows":0},"not_found":{"movies":1,"shows":0}}`)
	})
	response := applyEvents(t, s,
		movieEvent("m1", removeFromWatchlist, map[string]string{"imdb": "tt0111161"}),
		seriesEvent("s1", removeFromWatchlist, map[string]string{"tmdb": "1396"}),
	)
	assertStatus(t, response, "m1", statusNoChange)
	assertStatus(t, response, "s1", statusNoChange)
}

func TestWatchlistRejectsUnidentifiedItemWithoutCallingMDBList(t *testing.T) {
	s := newTestServer(t, func(http.ResponseWriter, *http.Request) {
		t.Error("MDBList should not be called for an unsupported item")
	})
	episode := episodeEvent("e1", addToWatchlist, map[string]string{"tvdb": "349232"}, nil, 1, 1)
	response := applyEvents(t, s, movieEvent("m1", addToWatchlist, nil), episode)
	assertStatus(t, response, "m1", statusRejected)
	assertStatus(t, response, "e1", statusRejected)
}
