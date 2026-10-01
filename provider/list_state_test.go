package provider

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	"google.golang.org/protobuf/proto"
)

const (
	kindWatched   = pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_WATCHED
	kindProgress  = pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_PROGRESS
	kindWatchlist = pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_WATCHLIST
	kindRating    = pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_RATING
)

// TestProviderItemKeysMatchBuiltInProvider pins the key formats Silo has
// stored for existing MDBList connections.
func TestProviderItemKeysMatchBuiltInProvider(t *testing.T) {
	cases := []struct {
		name string
		got  string
		want string
	}{
		{"movie prefers IMDb", movieKey(mdblistIDs{IMDb: "tt2049403", TMDB: 917496}), "imdb:tt2049403"},
		{"movie then TMDB", movieKey(mdblistIDs{TMDB: 278, TVDB: 9}), "tmdb:278"},
		{"movie then TVDB", movieKey(mdblistIDs{TVDB: 9}), "tvdb:9"},
		{"movie then MDBList", movieKey(mdblistIDs{MDBList: "a388", Trakt: 5}), "mdblist:a388"},
		{"movie without IDs", movieKey(mdblistIDs{Trakt: 5}), ""},
		{"show prefers TVDB", showKey(mdblistIDs{IMDb: "tt0903747", TMDB: 1396, TVDB: 81189}), "tvdb:81189"},
		{"show then TMDB", showKey(mdblistIDs{IMDb: "tt0903747", TMDB: 1396}), "tmdb:1396"},
		{"show then IMDb", showKey(mdblistIDs{IMDb: "tt0903747", MDBList: "8plj"}), "imdb:tt0903747"},
		{"show then MDBList", showKey(mdblistIDs{MDBList: "8plj"}), "mdblist:8plj"},
		{"episode TVDB", episodeKey(mdblistIDs{TVDB: 81189}, 1, 2, mdblistIDs{TVDB: 349232, TMDB: 62086}), "tvdb:349232"},
		{"episode TMDB", episodeKey(mdblistIDs{TVDB: 81189}, 1, 2, mdblistIDs{TMDB: 62086, IMDb: "tt1054724"}), "tmdb:62086"},
		{"episode IMDb", episodeKey(mdblistIDs{TVDB: 81189}, 1, 2, mdblistIDs{IMDb: "tt1054724"}), "imdb:tt1054724"},
		{"episode by show TVDB", episodeKey(mdblistIDs{TVDB: 81189, TMDB: 1396}, 1, 2, mdblistIDs{}), "show:tvdb:81189:s1:e2"},
		{"episode by show TMDB", episodeKey(mdblistIDs{TMDB: 1396, IMDb: "tt0903747"}, 0, 3, mdblistIDs{}), "show:tmdb:1396:s0:e3"},
		{"episode by show IMDb", episodeKey(mdblistIDs{IMDb: "tt0903747"}, 5, 16, mdblistIDs{}), "show:imdb:tt0903747:s5:e16"},
		{"episode without IDs", episodeKey(mdblistIDs{MDBList: "8plj"}, 1, 1, mdblistIDs{}), ""},
	}
	for _, tc := range cases {
		if tc.got != tc.want {
			t.Errorf("%s: key = %q, want %q", tc.name, tc.got, tc.want)
		}
	}
}

func TestListWatchedImportsPlayableLeavesWithoutExpandingAggregateRows(t *testing.T) {
	watched := time.Date(2025, time.October, 21, 12, 0, 0, 0, time.UTC)
	s := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/sync/watched" || r.URL.Query().Get("plays") != "all" || r.URL.Query().Get("limit") != "1000" {
			t.Errorf("request = %s?%s, want /sync/watched with plays=all and limit=1000", r.URL.Path, r.URL.RawQuery)
		}
		stamp := watched.Format(time.RFC3339)
		writeJSON(w, `{
			"movies":[{"last_watched_at":"`+stamp+`","movie":{"title":"Beetlejuice Beetlejuice","year":2024,"ids":{"imdb":"tt2049403","tmdb":917496}}}],
			"shows":[{"last_watched_at":"`+stamp+`","show":{"title":"Breaking Bad","year":2008,"ids":{"tvdb":81189,"tmdb":1396}}}],
			"seasons":[{"last_watched_at":"`+stamp+`","season":{"number":2,"name":"Season 2","ids":{"tmdb":8160},"show":{"title":"Breaking Bad","year":2008,"ids":{"imdb":"tt0903747","tmdb":1396}}}}],
			"episodes":[{"last_watched_at":"`+stamp+`","episode":{"season":1,"number":2,"name":"Cat's in the Bag...","ids":{"tmdb":62086},"show":{"title":"Breaking Bad","year":2008,"ids":{"tvdb":81189,"imdb":"tt0903747"}}}}],
			"pagination":{"has_more":false}
		}`)
	})
	result := listAll(t, s, kindWatched)
	if result.fault != nil || len(result.items) != 2 {
		t.Fatalf("traversal = %+v, want two leaf plays", result)
	}
	movie, episode := result.items[0], result.items[1]
	if movie.GetProviderItemKey() != "imdb:tt2049403" || movie.GetMedia().GetMediaType() != pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_MOVIE ||
		movie.GetMedia().GetTitle() != "Beetlejuice Beetlejuice" || movie.GetMedia().GetYear() != 2024 ||
		movie.GetMedia().GetExternalIds()["imdb"] != "tt2049403" || movie.GetMedia().GetExternalIds()["tmdb"] != "917496" ||
		len(movie.GetMedia().GetExternalIds()) != 2 {
		t.Fatalf("movie = %v", movie)
	}
	media := episode.GetMedia()
	if episode.GetProviderItemKey() != "tmdb:62086" || media.GetMediaType() != pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE ||
		media.GetSeasonNumber() != 1 || media.GetEpisodeNumber() != 2 || media.GetTitle() != "Cat's in the Bag..." ||
		media.GetExternalIds()["tmdb"] != "62086" || media.GetSeriesTitle() != "Breaking Bad" || media.GetSeriesYear() != 2008 ||
		media.GetSeriesExternalIds()["imdb"] != "tt0903747" || media.GetSeriesExternalIds()["tvdb"] != "81189" {
		t.Fatalf("episode = %v", episode)
	}
	for _, item := range result.items {
		if item.GetWatched().GetPlayCount() != 1 || !item.GetWatched().GetLastWatchedAt().AsTime().Equal(watched) {
			t.Fatalf("watched state = %v, want one play at %s", item.GetWatched(), watched)
		}
	}
	if !result.complete {
		t.Fatal("a full history read should be a complete snapshot")
	}
}

func TestListWatchedAcceptsLegacyWatchedAtAndFlatEpisode(t *testing.T) {
	watched := time.Date(2025, time.November, 1, 8, 30, 0, 0, time.UTC)
	s := newTestServer(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, `{
			"movies":[{"watched_at":"2025-11-01T08:30:00Z","movie":{"ids":{"imdb":"tt0111161"}}}],
			"episodes":[{"watched_at":"2025-11-01T08:30:00Z","season":1,"number":2,"show":{"ids":{"tvdb":81189}}}]
		}`)
	})
	result := listAll(t, s, kindWatched)
	if len(result.items) != 2 || result.items[1].GetProviderItemKey() != "show:tvdb:81189:s1:e2" {
		t.Fatalf("items = %v", result.items)
	}
	for _, item := range result.items {
		if !item.GetWatched().GetLastWatchedAt().AsTime().Equal(watched) {
			t.Fatalf("watched at = %v, want %s", item.GetWatched().GetLastWatchedAt().AsTime(), watched)
		}
	}
}

func TestListWatchedFollowsCursorAcrossPages(t *testing.T) {
	var queries []string
	s := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		queries = append(queries, r.URL.RawQuery)
		if r.URL.Query().Get("cursor") == "next-page" {
			writeJSON(w, `{"movies":[{"watched_at":"2025-11-01T08:30:00Z","movie":{"ids":{"tmdb":2}}}],"episodes":[],"pagination":{"next_cursor":null}}`)
			return
		}
		writeJSON(w, `{"movies":[{"watched_at":"2025-11-01T08:30:00Z","movie":{"ids":{"tmdb":1}}}],"episodes":[],"pagination":{"next_cursor":"next-page"}}`)
	})
	result := listAll(t, s, kindWatched)
	if result.pages != 2 || len(result.items) != 2 || len(queries) != 2 {
		t.Fatalf("pages = %d items = %d queries = %v", result.pages, len(result.items), queries)
	}
	if strings.Contains(queries[0], "offset=") || strings.Contains(queries[0], "cursor=") || !strings.Contains(queries[1], "cursor=next-page") {
		t.Fatalf("unexpected pagination queries: %#v", queries)
	}
}

func TestListWatchedContinuesOffsetPaginationWhenHasMore(t *testing.T) {
	var offsets []string
	s := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		offsets = append(offsets, r.URL.Query().Get("offset"))
		if r.URL.Query().Get("offset") == "1" {
			writeJSON(w, `{"movies":[],"episodes":[],"pagination":{"has_more":false}}`)
			return
		}
		writeJSON(w, `{"movies":[{"watched_at":"2025-11-01T08:30:00Z","movie":{"ids":{"imdb":"tt0111161"}}}],"episodes":[],"pagination":{"has_more":true}}`)
	})
	result := listAll(t, s, kindWatched)
	if len(result.items) != 1 || len(offsets) != 2 || offsets[0] != "" || offsets[1] != "1" {
		t.Fatalf("items = %d offsets = %#v", len(result.items), offsets)
	}
}

func TestListWatchedOffsetCountsAggregateRows(t *testing.T) {
	var offsets []string
	s := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		offsets = append(offsets, r.URL.Query().Get("offset"))
		if r.URL.Query().Get("offset") == "2" {
			writeJSON(w, `{"movies":[],"shows":[],"seasons":[],"episodes":[],"pagination":{"has_more":false}}`)
			return
		}
		writeJSON(w, `{
			"shows":[{"last_watched_at":"2025-11-01T08:30:00Z","show":{"ids":{"tmdb":1396}}}],
			"seasons":[{"last_watched_at":"2025-11-01T08:30:00Z","season":{"number":1,"show":{"ids":{"tmdb":1396}}}}],
			"pagination":{"has_more":true}
		}`)
	})
	result := listAll(t, s, kindWatched)
	if len(result.items) != 0 || len(offsets) != 2 || offsets[1] != "2" {
		t.Fatalf("items = %d offsets = %#v, want aggregate rows counted but not imported", len(result.items), offsets)
	}
}

func TestListWatchedMovesFromOffsetToCursorPages(t *testing.T) {
	var queries []string
	s := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		queries = append(queries, r.URL.RawQuery)
		switch {
		case r.URL.Query().Get("cursor") == "c3":
			writeJSON(w, `{"movies":[{"watched_at":"2025-11-01T08:30:00Z","movie":{"ids":{"tmdb":3}}}],"pagination":{"next_cursor":null}}`)
		case r.URL.Query().Get("offset") == "1":
			writeJSON(w, `{"movies":[{"watched_at":"2025-11-01T08:30:00Z","movie":{"ids":{"tmdb":2}}}],"pagination":{"next_cursor":"c3"}}`)
		default:
			writeJSON(w, `{"movies":[{"watched_at":"2025-11-01T08:30:00Z","movie":{"ids":{"tmdb":1}}}],"pagination":{"has_more":true}}`)
		}
	})
	result := listAll(t, s, kindWatched)
	if result.fault != nil || len(result.items) != 3 || len(queries) != 3 {
		t.Fatalf("traversal = %+v queries = %v", result, queries)
	}
	if strings.Contains(queries[2], "offset=") {
		t.Fatalf("cursor page query = %q, want the cursor to replace the offset", queries[2])
	}
}

func TestListWatchedFailsTraversalOnRepeatedCursor(t *testing.T) {
	s := newTestServer(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, `{"movies":[],"pagination":{"next_cursor":"same"}}`)
	})
	result := listAll(t, s, kindWatched)
	if result.fault.GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_TEMPORARY || result.pages != 2 {
		t.Fatalf("traversal = %+v, want a TEMPORARY fault on the second page", result)
	}
}

func TestListWatchedPageStaysFarBelowTheRPCSizeLimit(t *testing.T) {
	var episodes []string
	for i := range syncPageLimit {
		episodes = append(episodes, fmt.Sprintf(`{"watched_at":"2025-11-01T08:30:00Z","episode":{"season":12,"number":%d,"name":"An episode title of ordinary length","ids":{"tmdb":%d,"tvdb":%d,"imdb":"tt%07d"},"show":{"title":"A series title of ordinary length","year":2008,"ids":{"tmdb":1396,"tvdb":81189,"imdb":"tt0903747"}}}}`, i, 1000000+i, 2000000+i, i))
	}
	body := `{"episodes":[` + strings.Join(episodes, ",") + `],"pagination":{"next_cursor":"more"}}`
	s := newTestServer(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, body)
	})
	response, _ := s.ListRemoteState(context.Background(), &pluginv1.WatchSyncListRemoteStateRequest{
		Context: authContext(testKey), StateKinds: []pluginv1.WatchSyncRemoteStateKind{kindWatched},
	})
	if len(response.GetItems()) != syncPageLimit {
		t.Fatalf("items = %d", len(response.GetItems()))
	}
	if size := proto.Size(response); size > 1<<20 {
		t.Fatalf("a full page is %d bytes; it must stay well under the 4 MiB gRPC limit", size)
	}
}

func TestListProgressParsesFlatArray(t *testing.T) {
	pausedAt := time.Date(2025, time.November, 1, 8, 30, 0, 0, time.UTC)
	s := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/sync/playback" {
			t.Errorf("path = %q", r.URL.Path)
		}
		writeJSON(w, `[
			{"progress":42.5,"paused_at":"2025-11-01T08:30:00Z","action":"pause","movie":{"title":"Sample","ids":{"tmdb":278}}},
			{"progress":5,"action":"start","movie":{"ids":{"tmdb":999}}},
			{"progress":"12%","paused_at":"2025-11-01T08:30:00Z","episode":{"season":1,"number":2,"title":"Pilot","ids":{"tvdb":349232}},"show":{"title":"Breaking Bad","ids":{"tvdb":81189}}}
		]`)
	})
	result := listAll(t, s, kindProgress)
	if result.fault != nil || len(result.items) != 2 || !result.complete {
		t.Fatalf("traversal = %+v, want the paused movie and episode", result)
	}
	movie, episode := result.items[0], result.items[1]
	if movie.GetProviderItemKey() != "tmdb:278" || movie.GetProgress().GetProgressPercent() != 42.5 ||
		!movie.GetProgress().GetPausedAt().AsTime().Equal(pausedAt) || movie.GetMedia().GetTitle() != "Sample" {
		t.Fatalf("movie = %v", movie)
	}
	if episode.GetProviderItemKey() != "tvdb:349232" || episode.GetProgress().GetProgressPercent() != 12 ||
		episode.GetMedia().GetSeriesExternalIds()["tvdb"] != "81189" || episode.GetMedia().GetSeasonNumber() != 1 {
		t.Fatalf("episode = %v", episode)
	}
}

func TestListProgressParsesNestedObject(t *testing.T) {
	s := newTestServer(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, `{
			"paused":[{"progress":42.5,"paused_at":"2025-11-01T08:30:00Z","movie":{"ids":{"tmdb":278}}}],
			"scrobbling":[{"progress":5,"movie":{"ids":{"tmdb":999}}}]
		}`)
	})
	result := listAll(t, s, kindProgress)
	if len(result.items) != 1 || result.items[0].GetProviderItemKey() != "tmdb:278" {
		t.Fatalf("items = %v, want only the paused row", result.items)
	}
}

func TestListProgressStampsMissingPauseTimeAndKeepsPercentBelowHundred(t *testing.T) {
	now := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	s := newTestServer(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, `[{"progress":100,"movie":{"ids":{"tmdb":278}}},{"progress":-1,"movie":{"ids":{"tmdb":1}}}]`)
	})
	s.now = func() time.Time { return now }
	result := listAll(t, s, kindProgress)
	if len(result.items) != 1 {
		t.Fatalf("items = %v", result.items)
	}
	progress := result.items[0].GetProgress()
	if !progress.GetPausedAt().AsTime().Equal(now) || progress.GetProgressPercent() >= 100 || progress.GetProgressPercent() < 99.99 {
		t.Fatalf("progress = %v", progress)
	}
}

func TestListWatchlistMapsMoviesAndShowsInOrder(t *testing.T) {
	s := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/watchlist/items" {
			t.Errorf("path = %q", r.URL.Path)
		}
		writeJSON(w, `{
			"movies":[
				{"id":278,"title":"The Shawshank Redemption","mediatype":"movie","release_year":1994,"imdb_id":"tt0111161"},
				{"id":550,"title":"Fight Club","mediatype":"movie","release_year":1999}
			],
			"shows":[{"id":1396,"title":"Breaking Bad","mediatype":"show","release_year":2008,"tvdb_id":81189,"imdb_id":"tt0903747"}],
			"pagination":{"next_cursor":null}
		}`)
	})
	result := listAll(t, s, kindWatchlist)
	if result.fault != nil || !result.complete || len(result.items) != 3 {
		t.Fatalf("traversal = %+v", result)
	}
	want := []struct {
		key       string
		mediaType pluginv1.WatchSyncMediaType
		ids       map[string]string
	}{
		{"imdb:tt0111161", pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_MOVIE, map[string]string{"imdb": "tt0111161", "tmdb": "278"}},
		{"tmdb:550", pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_MOVIE, map[string]string{"tmdb": "550"}},
		{"tvdb:81189", pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_SERIES, map[string]string{"imdb": "tt0903747", "tmdb": "1396", "tvdb": "81189"}},
	}
	for i, item := range result.items {
		if item.GetProviderItemKey() != want[i].key || item.GetMedia().GetMediaType() != want[i].mediaType ||
			fmt.Sprint(item.GetMedia().GetExternalIds()) != fmt.Sprint(want[i].ids) || item.GetWatchlist() == nil ||
			item.GetWatchlist().GetListedAt() != nil || item.GetWatchlist().GetRemoved() {
			t.Fatalf("item %d = %v, want %+v", i, item, want[i])
		}
	}
	if result.items[0].GetMedia().GetYear() != 1994 || result.items[0].GetMedia().GetTitle() != "The Shawshank Redemption" {
		t.Fatalf("movie media = %v", result.items[0].GetMedia())
	}
}

func TestListWatchlistUsesCursorPagination(t *testing.T) {
	var cursors []string
	s := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		cursors = append(cursors, r.URL.Query().Get("cursor"))
		if r.URL.Query().Get("cursor") == "watch-next" {
			writeJSON(w, `{"movies":[],"shows":[],"pagination":{"next_cursor":null}}`)
			return
		}
		writeJSON(w, `{"movies":[],"shows":[],"pagination":{"next_cursor":"watch-next"}}`)
	})
	result := listAll(t, s, kindWatchlist)
	if !result.complete || len(cursors) != 2 || cursors[0] != "" || cursors[1] != "watch-next" {
		t.Fatalf("complete = %t cursors = %#v", result.complete, cursors)
	}
}

func TestListWatchlistContinuesOffsetPaginationWhenHasMore(t *testing.T) {
	var offsets []string
	s := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		offsets = append(offsets, r.URL.Query().Get("offset"))
		if r.URL.Query().Get("offset") == "1" {
			writeJSON(w, `{"movies":[],"shows":[],"pagination":{"has_more":false}}`)
			return
		}
		writeJSON(w, `{"movies":[{"title":"The Shawshank Redemption","release_year":1994,"ids":{"imdb":"tt0111161"}}],"shows":[],"pagination":{"has_more":true}}`)
	})
	result := listAll(t, s, kindWatchlist)
	if len(result.items) != 1 || len(offsets) != 2 || offsets[0] != "" || offsets[1] != "1" {
		t.Fatalf("items = %d offsets = %#v", len(result.items), offsets)
	}
}

func TestListWatchlistIgnoresPaginationTotal(t *testing.T) {
	// Only the ratings read checks total; the watchlist read still ends at
	// the first page without next_cursor or has_more.
	requests := 0
	s := newTestServer(t, func(w http.ResponseWriter, _ *http.Request) {
		requests++
		writeJSON(w, `{"movies":[{"title":"The Shawshank Redemption","release_year":1994,"ids":{"imdb":"tt0111161"}}],"shows":[],"pagination":{"total":5,"limit":1000,"next_cursor":null}}`)
	})
	result := listAll(t, s, kindWatchlist)
	if requests != 1 || len(result.items) != 1 {
		t.Fatalf("requests = %d items = %d, want one page and one item", requests, len(result.items))
	}
}
