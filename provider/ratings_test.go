package provider

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
)

const (
	setRating    = pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SET_RATING
	removeRating = pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_REMOVE_RATING

	mediaMovie   = pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_MOVIE
	mediaSeries  = pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_SERIES
	mediaEpisode = pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE
)

func TestListRatingsUsesCursorPagination(t *testing.T) {
	var queries []string
	s := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/sync/ratings" {
			t.Errorf("path = %q, want /sync/ratings", r.URL.Path)
		}
		queries = append(queries, r.URL.RawQuery)
		if r.URL.Query().Get("cursor") == "ratings-next" {
			// The second page has no shows list; the first page's list still
			// makes the show ratings part of the snapshot.
			writeJSON(w, `{"movies":[{"rated_at":"2025-10-22T09:00:00Z","rating":6,"movie":{"title":"Heat","year":1995,"ids":{"tmdb":"949"}}}],"pagination":{"next_cursor":null}}`)
			return
		}
		writeJSON(w, `{
			"movies":[
				{"rated_at":"2025-10-21T14:00:00Z","rating":8,"movie":{"title":"The Avengers","year":2012,"ids":{"trakt":24428,"imdb":"tt0848228","tmdb":24428,"kitsu":67890}}},
				{"rated_at":null,"rating":null,"movie":{"title":"Unrated","year":2001,"ids":{"imdb":"tt0000001"}}},
				{"rated_at":null,"rating":0,"movie":{"title":"Cleared","year":2002,"ids":{"imdb":"tt0000002"}}}
			],
			"shows":[{"rated_at":"2025-10-20T15:00:00Z","rating":9.0,"show":{"title":"Breaking Bad","year":2008,"ids":{"imdb":"tt0903747","tmdb":1396,"tvdb":81189}}}],
			"seasons":[{"rated_at":"2025-10-15T20:00:00Z","rating":8,"season":{"number":1,"show":{"ids":{"tmdb":1396}}}}],
			"episodes":[{"rated_at":"2025-10-15T21:00:00Z","rating":10,"episode":{"season":1,"number":1,"ids":{"tmdb":62085},"show":{"ids":{"tmdb":1396}}}}],
			"pagination":{"total":4,"limit":1000,"next_cursor":"ratings-next"}
		}`)
	})
	result := listAll(t, s, kindRating)
	if result.fault != nil {
		t.Fatalf("fault = %v", result.fault)
	}
	if len(queries) != 2 || strings.Contains(queries[0], "cursor=") || !strings.Contains(queries[1], "cursor=ratings-next") {
		t.Fatalf("unexpected pagination queries: %#v", queries)
	}
	for _, query := range queries {
		if !strings.Contains(query, "limit=1000") {
			t.Fatalf("query %q does not request 1000 items", query)
		}
	}
	if !result.complete {
		t.Fatal("a clean cursor read with a shows list should be a complete snapshot")
	}
	if len(result.items) != 4 {
		t.Fatalf("items = %v, want two rated movies, one show and one episode", result.items)
	}
	avengers, show, episode, heat := result.items[0], result.items[1], result.items[2], result.items[3]
	// The rated season is counted but never returned: the plugin contract has
	// no season media type, so Silo cannot name one.
	if episode.GetMedia().GetMediaType() != mediaEpisode || episode.GetRating().GetRating() != 10 ||
		episode.GetProviderItemKey() != "tmdb:62085" ||
		episode.GetMedia().GetExternalIds()["tmdb"] != "62085" ||
		episode.GetMedia().GetSeriesExternalIds()["tmdb"] != "1396" ||
		episode.GetMedia().GetSeasonNumber() != 1 || episode.GetMedia().GetEpisodeNumber() != 1 {
		t.Fatalf("episode = %v", episode)
	}
	if avengers.GetMedia().GetMediaType() != mediaMovie || avengers.GetRating().GetRating() != 8 || avengers.GetProviderItemKey() != "imdb:tt0848228" ||
		avengers.GetMedia().GetExternalIds()["imdb"] != "tt0848228" || avengers.GetMedia().GetExternalIds()["tmdb"] != "24428" ||
		avengers.GetMedia().GetTitle() != "The Avengers" || avengers.GetMedia().GetYear() != 2012 ||
		!avengers.GetRating().GetRatedAt().AsTime().Equal(time.Date(2025, 10, 21, 14, 0, 0, 0, time.UTC)) {
		t.Fatalf("movie = %v", avengers)
	}
	if show.GetMedia().GetMediaType() != mediaSeries || show.GetRating().GetRating() != 9 || show.GetProviderItemKey() != "tvdb:81189" ||
		show.GetMedia().GetExternalIds()["tvdb"] != "81189" || show.GetMedia().GetExternalIds()["tmdb"] != "1396" ||
		show.GetMedia().GetExternalIds()["imdb"] != "tt0903747" {
		t.Fatalf("show = %v", show)
	}
	if heat.GetMedia().GetMediaType() != mediaMovie || heat.GetRating().GetRating() != 6 || heat.GetProviderItemKey() != "tmdb:949" {
		t.Fatalf("second page movie = %v", heat)
	}
}

func TestListRatingsIsNoSnapshotWithoutShowsList(t *testing.T) {
	cases := map[string]struct {
		body     string
		complete bool
	}{
		// The documented sample response: no shows key and legacy pagination.
		"documented sample": {
			body: `{"movies":[{"rated_at":"2025-10-21T14:00:00Z","rating":8,"movie":{"title":"The Avengers","year":2012,"ids":{"imdb":"tt0848228"}}}],"seasons":[],"episodes":[],"pagination":{"offset":0,"limit":1000,"total_movies":1,"has_more":false}}`,
		},
		"null shows":  {body: `{"movies":[],"shows":null,"pagination":{"next_cursor":null}}`},
		"empty shows": {body: `{"movies":[],"shows":[],"pagination":{"next_cursor":null}}`, complete: true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			s := newTestServer(t, func(w http.ResponseWriter, _ *http.Request) {
				writeJSON(w, tc.body)
			})
			result := listAll(t, s, kindRating)
			if result.fault != nil || result.complete != tc.complete {
				t.Fatalf("complete = %t fault = %v, want complete %t", result.complete, result.fault, tc.complete)
			}
			// An incomplete read says why, so the sync run shows it.
			if !tc.complete && !slices.Contains(result.warnings, ratingsNoShowsWarning) {
				t.Fatalf("warnings = %q, want the missing shows list explained", result.warnings)
			}
			if tc.complete && len(result.warnings) != 0 {
				t.Fatalf("warnings = %q, want none for a complete read", result.warnings)
			}
		})
	}
}

func TestListRatingsReturnsUnidentifiedTitlesWithoutExternalIDs(t *testing.T) {
	s := newTestServer(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, `{
			"movies":[
				{"rating":7,"movie":{"title":"MDBList only","ids":{"mdblist":"8plj","trakt":5}}},
				{"rating":6,"movie":{"ids":{"imdb":"tt0113277"}}}
			],
			"shows":[
				{"rating":8,"show":{"title":"No IDs","ids":{"trakt":5}}},
				{"rating":9,"show":{"ids":{"tvdb":81189,"mdblist":"9abc"}}}
			],
			"pagination":{"next_cursor":null}
		}`)
	})
	result := listAll(t, s, kindRating)
	if result.fault != nil || !result.complete || len(result.items) != 4 {
		t.Fatalf("traversal = %+v", result)
	}
	keys := make([]string, 0, len(result.items))
	for _, item := range result.items {
		keys = append(keys, item.GetProviderItemKey())
	}
	if keys[0] != "mdblist:8plj" || keys[1] != "imdb:tt0113277" || !strings.HasPrefix(keys[2], "unidentified:") || keys[3] != "tvdb:81189" {
		t.Fatalf("keys = %v", keys)
	}
	// Silo leaves a kind out of a snapshot when a rating of that kind has no
	// IMDb, TMDB, or TVDB ID, the way the built-in provider reported it.
	for _, index := range []int{0, 2} {
		item := result.items[index]
		if len(item.GetMedia().GetExternalIds()) != 0 || item.GetRating().GetRating() == 0 {
			t.Fatalf("unidentified item = %v, want a rating without external IDs", item)
		}
	}
	if result.items[2].GetMedia().GetMediaType() != mediaSeries || result.items[2].GetMedia().GetTitle() != "No IDs" {
		t.Fatalf("unidentified show = %v", result.items[2])
	}
}

// ratingsPage renders one ratings page of movies, shows, and episodes with
// the given pagination. Entries are numbered from first so pages do not
// repeat an entry.
func ratingsPage(first, movies, shows, episodes int, pagination string) string {
	var movieRows, showRows, episodeRows []string
	for i := range movies {
		movieRows = append(movieRows, fmt.Sprintf(`{"rating":7,"movie":{"ids":{"tmdb":%d}}}`, first+i+1))
	}
	for i := range shows {
		showRows = append(showRows, fmt.Sprintf(`{"rating":8,"show":{"ids":{"tvdb":%d}}}`, first+movies+i+1))
	}
	for i := range episodes {
		episodeRows = append(episodeRows, fmt.Sprintf(`{"rating":9,"episode":{"ids":{"tmdb":%d}}}`, first+movies+shows+i+1))
	}
	return fmt.Sprintf(`{"movies":[%s],"shows":[%s],"episodes":[%s],"pagination":%s}`,
		strings.Join(movieRows, ","), strings.Join(showRows, ","), strings.Join(episodeRows, ","), pagination)
}

// ratingsFake serves pages keyed "cursor|offset"; any other request gets an
// empty page.
func ratingsFake(t *testing.T, pages map[string]string, requested *[]string) *Server {
	t.Helper()
	return newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("cursor") + "|" + r.URL.Query().Get("offset")
		*requested = append(*requested, key)
		body, ok := pages[key]
		if !ok {
			body = `{"movies":[],"shows":[],"pagination":{"next_cursor":null}}`
		}
		writeJSON(w, body)
	})
}

func TestListRatingsPagesByOffsetUntilTotal(t *testing.T) {
	// Offset pagination without next_cursor or has_more: only total says
	// that more entries remain. Each page mixes movies, shows, and episodes,
	// and the offset counts all of them.
	var requested []string
	s := ratingsFake(t, map[string]string{
		"|":     ratingsPage(0, 500, 250, 250, `{"total":2500,"limit":1000,"offset":0,"next_cursor":null}`),
		"|1000": ratingsPage(1000, 500, 250, 250, `{"total":2500,"limit":1000,"offset":1000,"next_cursor":null}`),
		"|2000": ratingsPage(2000, 250, 125, 125, `{"total":2500,"limit":1000,"offset":2000,"next_cursor":null}`),
	}, &requested)
	result := listAll(t, s, kindRating)
	if !slices.Equal(requested, []string{"|", "|1000", "|2000"}) {
		t.Fatalf("pages = %#v, want three offset pages", requested)
	}
	if len(result.items) != 2500 {
		t.Fatalf("items = %d, want every rated movie, show and episode", len(result.items))
	}
	// Offset pages can shift under a concurrent change, so the read imports
	// what it saw but claims no snapshot, and says why.
	if result.fault != nil || result.complete {
		t.Fatalf("complete = %t fault = %v, want an incremental read", result.complete, result.fault)
	}
	if !slices.Equal(result.warnings, []string{ratingsOffsetPagedWarning}) {
		t.Fatalf("warnings = %q, want the offset read explained", result.warnings)
	}
}

func TestListRatingsShortOfTotal(t *testing.T) {
	cases := map[string]struct {
		pages     map[string]string
		wantPages []string
		wantItems int
		// wantFault: the traversal claimed a snapshot on its first page, so a
		// short read fails it instead of leaving titles out.
		wantFault bool
		// wantWarnings, when set, are the warnings the read must return.
		wantWarnings []string
	}{
		"cursor read ends short": {
			pages: map[string]string{
				"|":   ratingsPage(0, 2, 0, 0, `{"total":5,"limit":1000,"next_cursor":"c2"}`),
				"c2|": ratingsPage(2, 0, 1, 0, `{"total":5,"limit":1000,"next_cursor":null}`),
			},
			wantPages: []string{"|", "c2|"},
			wantFault: true,
		},
		"offset read ends short": {
			pages: map[string]string{
				"|": ratingsPage(0, 1, 1, 1, `{"total":10,"limit":1000,"offset":0,"next_cursor":null}`),
			},
			wantPages: []string{"|", "|3"},
			wantItems: 3,
		},
		"legacy per-type totals": {
			pages: map[string]string{
				"|": `{"movies":[{"rating":8,"movie":{"ids":{"imdb":"tt0848228"}}}],"shows":[],"seasons":[],"episodes":[],"pagination":{"offset":0,"limit":1000,"total_movies":2,"total_seasons":1,"has_more":false}}`,
			},
			wantPages: []string{"|", "|1"},
			wantItems: 1,
		},
		"single page ends short": {
			pages: map[string]string{
				"|": `{"movies":[],"shows":[],"pagination":{"total":3,"limit":1000,"next_cursor":null}}`,
			},
			wantPages:    []string{"|"},
			wantWarnings: []string{"mdblist ratings read ended after 0 of 3 entries; skipped rating removals"},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			var requested []string
			result := listAll(t, ratingsFake(t, tc.pages, &requested), kindRating)
			if !slices.Equal(requested, tc.wantPages) {
				t.Fatalf("pages = %#v, want %#v", requested, tc.wantPages)
			}
			if tc.wantFault {
				if result.fault.GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_TEMPORARY {
					t.Fatalf("fault = %v, want TEMPORARY", result.fault)
				}
				return
			}
			if result.fault != nil || result.complete || len(result.items) != tc.wantItems {
				t.Fatalf("complete = %t fault = %v items = %d, want an incremental read of %d", result.complete, result.fault, len(result.items), tc.wantItems)
			}
			if len(result.warnings) == 0 {
				t.Fatal("an incremental read returned no warning explaining it")
			}
			if tc.wantWarnings != nil && !slices.Equal(result.warnings, tc.wantWarnings) {
				t.Fatalf("warnings = %q, want %q", result.warnings, tc.wantWarnings)
			}
		})
	}
}

func TestListRatingsRepeatedEntry(t *testing.T) {
	cases := map[string]struct {
		pages     map[string]string
		complete  bool
		wantItems int
		wantFault bool
	}{
		// Offsets can shift without a repeat: B removed and X added after
		// the boundary skips an entry while the count still reaches total.
		"offset read without a repeat": {
			pages: map[string]string{
				"|":  ratingsPage(0, 2, 1, 0, `{"total":5,"limit":3,"offset":0,"next_cursor":null}`),
				"|3": ratingsPage(3, 1, 1, 0, `{"total":5,"limit":3,"offset":3,"next_cursor":null}`),
			},
			wantItems: 5,
		},
		"clean cursor read": {
			pages: map[string]string{
				"|":   ratingsPage(0, 2, 1, 0, `{"total":5,"limit":3,"next_cursor":"c2"}`),
				"c2|": ratingsPage(3, 1, 1, 0, `{"total":5,"limit":3,"next_cursor":null}`),
			},
			complete:  true,
			wantItems: 5,
		},
		// A rating added between the requests shifts page 2 by one: it
		// repeats page 1's show (tvdb 3) and the read never sees one entry,
		// yet the count still reaches total.
		"offset read repeats a rated title": {
			pages: map[string]string{
				"|":  ratingsPage(0, 2, 1, 0, `{"total":5,"limit":3,"offset":0,"next_cursor":null}`),
				"|3": `{"movies":[],"shows":[{"rating":8,"show":{"ids":{"tvdb":3}}},{"rating":8,"show":{"ids":{"tvdb":5}}}],"pagination":{"total":5,"limit":3,"offset":3,"next_cursor":null}}`,
			},
			wantItems: 5,
		},
		// A rated season is counted but never becomes a rating row, because the
		// plugin contract has no season media type. A repeat of one therefore
		// shows the ratings changed without the read losing an item.
		"offset read repeats an entry without a rating row": {
			pages: map[string]string{
				"|":  `{"movies":[{"rating":7,"movie":{"ids":{"tmdb":1}}}],"shows":[],"seasons":[{"rating":8,"season":{"number":1,"show":{"ids":{"tmdb":1396}}}}],"pagination":{"total":4,"limit":2,"offset":0,"next_cursor":null}}`,
				"|2": `{"movies":[{"rating":7,"movie":{"ids":{"tmdb":4}}}],"shows":[],"seasons":[{"rating":8,"season":{"number":1,"show":{"ids":{"tmdb":1396}}}}],"pagination":{"total":4,"limit":2,"offset":2,"next_cursor":null}}`,
			},
			wantItems: 2,
		},
		"cursor read repeats a rated title": {
			pages: map[string]string{
				"|":   ratingsPage(0, 2, 1, 0, `{"total":5,"limit":3,"next_cursor":"c2"}`),
				"c2|": `{"movies":[{"rating":7,"movie":{"ids":{"tmdb":2}}},{"rating":7,"movie":{"ids":{"tmdb":4}}}],"shows":[],"pagination":{"total":5,"limit":3,"next_cursor":null}}`,
			},
			wantFault: true,
		},
		"cursor read repeats an entry without a rating row": {
			pages: map[string]string{
				"|":   `{"movies":[],"shows":[],"seasons":[{"rating":8,"season":{"number":1,"show":{"ids":{"tmdb":1396}}}}],"pagination":{"total":3,"limit":1,"next_cursor":"c2"}}`,
				"c2|": `{"movies":[{"rating":7,"movie":{"ids":{"tmdb":4}}}],"shows":[],"seasons":[{"rating":8,"season":{"number":1,"show":{"ids":{"tmdb":1396}}}}],"pagination":{"total":3,"limit":2,"next_cursor":null}}`,
			},
			wantFault: true,
		},
		"first page repeats an entry": {
			pages: map[string]string{
				"|": `{"movies":[{"rating":7,"movie":{"ids":{"tmdb":2}}},{"rating":7,"movie":{"ids":{"tmdb":2}}}],"shows":[],"pagination":{"total":2,"next_cursor":null}}`,
			},
			wantItems: 2,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			var requested []string
			result := listAll(t, ratingsFake(t, tc.pages, &requested), kindRating)
			if tc.wantFault {
				if result.fault.GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_TEMPORARY ||
					!strings.Contains(result.fault.GetSafeMessage(), "changed") {
					t.Fatalf("fault = %v, want a TEMPORARY ratings-changed fault", result.fault)
				}
				return
			}
			if result.fault != nil || result.complete != tc.complete || len(result.items) != tc.wantItems {
				t.Fatalf("complete = %t fault = %v items = %d, want complete %t and %d items",
					result.complete, result.fault, len(result.items), tc.complete, tc.wantItems)
			}
		})
	}
}

func TestListRatingsCarriesSeenEntriesAcrossManyCursorPages(t *testing.T) {
	pages := map[string]string{}
	for page := range 5 {
		cursor := ""
		if page > 0 {
			cursor = fmt.Sprintf("c%d", page)
		}
		next := fmt.Sprintf(`"c%d"`, page+1)
		if page == 4 {
			next = "null"
		}
		pages[cursor+"|"] = ratingsPage(page*1000, 600, 300, 100, `{"total":5000,"limit":1000,"next_cursor":`+next+`}`)
	}
	var requested []string
	result := listAll(t, ratingsFake(t, pages, &requested), kindRating)
	if result.fault != nil || !result.complete || len(result.items) != 5000 || len(requested) != 5 {
		t.Fatalf("complete = %t fault = %v items = %d pages = %d", result.complete, result.fault, len(result.items), len(requested))
	}
}

func TestSetRatingSendsRatingPayload(t *testing.T) {
	var gotPath, gotMethod string
	var bodies [][]byte
	s := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotMethod = r.URL.Path, r.Method
		body, _ := io.ReadAll(r.Body)
		bodies = append(bodies, body)
		writeJSON(w, `{"updated":{"movies":1,"shows":1},"not_found":{"movies":0,"shows":0},"errors":[]}`)
	})
	ratedAt := time.Date(2025, 10, 21, 16, 0, 0, 0, time.FixedZone("CEST", 2*60*60))
	movie := at(movieEvent("m1", setRating, map[string]string{"imdb": "tt0848228", "tmdb": "24428"}), ratedAt)
	movie.Rating = 8
	show := seriesEvent("s1", setRating, nil)
	show.ProviderItemKey = "tvdb:81189"
	show.Rating = 10
	episode := episodeEvent("e1", setRating, map[string]string{"tmdb": "62085"},
		map[string]string{"tvdb": "81189"}, 1, 1)
	episode.Rating = 6
	noIDs := movieEvent("m2", setRating, nil)
	noIDs.Rating = 4
	outOfRange := movieEvent("m3", setRating, map[string]string{"imdb": "tt1"})
	outOfRange.Rating = 11
	response := applyEvents(t, s, movie, show, episode, noIDs, outOfRange)
	if gotMethod != http.MethodPost || gotPath != "/sync/ratings" {
		t.Fatalf("request = %s %s, want POST /sync/ratings", gotMethod, gotPath)
	}
	// Two requests: the flat movie and show entries, then the episode nested
	// in its show. Both reuse the "shows" key, so they cannot share one body.
	if len(bodies) != 2 {
		t.Fatalf("requests = %d, want the titles and then the episodes", len(bodies))
	}
	assertJSONEqual(t, bodies[0], `{
		"movies":[{"ids":{"imdb":"tt0848228","tmdb":24428},"rating":8,"rated_at":"2025-10-21T14:00:00Z"}],
		"shows":[{"ids":{"tvdb":81189},"rating":10}]
	}`)
	assertJSONEqual(t, bodies[1], `{
		"shows":[{"ids":{"tvdb":81189},"seasons":[{"number":1,"episodes":[{"number":1,"rating":6}]}]}]
	}`)
	assertStatus(t, response, "m1", statusApplied)
	assertStatus(t, response, "s1", statusApplied)
	assertStatus(t, response, "e1", statusApplied)
	// A title with no external ID and a rating outside 1 to 10 are rejected
	// before the request is built.
	for _, id := range []string{"m2", "m3"} {
		result := assertStatus(t, response, id, statusRejected)
		if result.GetFault().GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_REQUEST {
			t.Fatalf("%s fault = %v", id, result.GetFault())
		}
	}
}

// Several episodes of one season travel as a single show entry, and an
// episode MDBList cannot be positioned in (no series ID, or no numbers) is
// rejected rather than sent as an entry that would be ignored.
func TestSetRatingGroupsEpisodesUnderTheirShow(t *testing.T) {
	var gotBody []byte
	s := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		writeJSON(w, `{"updated":{"shows":2},"not_found":{"shows":0},"errors":[]}`)
	})
	first := episodeEvent("e1", setRating, nil, map[string]string{"tvdb": "81189"}, 2, 7)
	first.Rating = 9
	second := episodeEvent("e2", setRating, nil, map[string]string{"tvdb": "81189"}, 2, 8)
	second.Rating = 4
	// Its own ID is no help: the write addresses an episode through its show.
	noSeries := episodeEvent("e3", setRating, map[string]string{"tmdb": "62085"}, nil, 1, 1)
	noSeries.Rating = 7
	unnumbered := episodeEvent("e4", setRating, nil, map[string]string{"tvdb": "81189"}, 0, 0)
	unnumbered.Rating = 5

	response := applyEvents(t, s, first, second, noSeries, unnumbered)

	assertJSONEqual(t, gotBody, `{
		"shows":[{"ids":{"tvdb":81189},"seasons":[{"number":2,"episodes":[
			{"number":7,"rating":9},{"number":8,"rating":4}
		]}]}]
	}`)
	assertStatus(t, response, "e1", statusApplied)
	assertStatus(t, response, "e2", statusApplied)
	for _, id := range []string{"e3", "e4"} {
		result := assertStatus(t, response, id, statusRejected)
		if result.GetFault().GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_REQUEST {
			t.Fatalf("%s fault = %v", id, result.GetFault())
		}
	}
}

// MDBList ignores a payload shape it does not recognise without reporting an
// error, so a request it accepted while recording nothing must not be read as
// applied: Silo would store the rating as agreed and never send it again.
func TestSetRatingFailsWhenMDBListRecordsNothing(t *testing.T) {
	s := newTestServer(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, `{"updated":{"movies":0,"shows":0},"not_found":{},"errors":[]}`)
	})
	movie := movieEvent("m1", setRating, map[string]string{"imdb": "tt0848228"})
	movie.Rating = 8

	response := applyEvents(t, s, movie)

	result := assertStatus(t, response, "m1", statusRetry)
	if result.GetFault().GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_TEMPORARY {
		t.Fatalf("m1 fault = %v, want TEMPORARY so the next sync retries", result.GetFault())
	}
}

func TestRemoveRatingSendsIDsOnly(t *testing.T) {
	var gotPath string
	var gotBody []byte
	s := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotBody, _ = io.ReadAll(r.Body)
		writeJSON(w, `{"deleted":{"movies":1,"shows":1},"not_found":{"movies":0,"shows":0}}`)
	})
	movie := movieEvent("m1", removeRating, nil)
	movie.ProviderItemKey = "tmdb:24428"
	response := applyEvents(t, s, movie, seriesEvent("s1", removeRating, map[string]string{"imdb": "tt0903747"}))
	if gotPath != "/sync/ratings/remove" {
		t.Fatalf("path = %q, want /sync/ratings/remove", gotPath)
	}
	assertJSONEqual(t, gotBody, `{"movies":[{"ids":{"tmdb":24428}}],"shows":[{"ids":{"imdb":"tt0903747"}}]}`)
	assertStatus(t, response, "m1", statusApplied)
	assertStatus(t, response, "s1", statusApplied)
}

func TestRatingWritesAcceptDocumentedResponseShapes(t *testing.T) {
	cases := map[string]struct {
		removing bool
		body     string
	}{
		"set, sample updated counts":    {body: `{"updated":{"movies":1,"seasons":0,"episodes":0}}`},
		"set, schema counts":            {body: `{"updated":{"movies":1},"not_found":{"movies":0,"shows":0},"errors":[]}`},
		"set, empty not_found lists":    {body: `{"added":{"movies":1},"not_found":{"movies":[],"shows":[]}}`},
		"remove, sample removed counts": {removing: true, body: `{"removed":{"movies":1,"seasons":0,"episodes":0}}`},
		"remove, schema deleted counts": {removing: true, body: `{"deleted":{"movies":1},"not_found":{"movies":0,"shows":0}}`},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			response := writeRatings(t, tc.removing, tc.body, 1)
			assertStatus(t, response, "m1", statusApplied)
		})
	}
}

func TestSetRatingRetriesNotFoundBatch(t *testing.T) {
	response := writeRatings(t, false, `{"updated":{"movies":1},"not_found":{"movies":1,"shows":0}}`, 2)
	assertStatus(t, response, "m1", statusRetry)
	assertStatus(t, response, "s1", statusRetry)
}

func TestRemoveRatingTreatsNotFoundBatchAsReconciled(t *testing.T) {
	response := writeRatings(t, true, `{"deleted":{"movies":1},"not_found":{"movies":0,"shows":1}}`, 2)
	assertStatus(t, response, "m1", statusNoChange)
	assertStatus(t, response, "s1", statusNoChange)
}

func TestRatingWritesRetryBatchThatReportsErrors(t *testing.T) {
	for _, removing := range []bool{false, true} {
		t.Run(fmt.Sprintf("removing=%t", removing), func(t *testing.T) {
			response := writeRatings(t, removing, `{"updated":{"movies":1},"not_found":{"movies":1},"errors":[{"message":"invalid rating"}]}`, 2)
			for _, id := range []string{"m1", "s1"} {
				result := assertStatus(t, response, id, statusRetry)
				if strings.Contains(result.GetFault().GetSafeMessage(), "invalid rating") {
					t.Fatalf("fault quotes the upstream response: %q", result.GetFault().GetSafeMessage())
				}
			}
		})
	}
}

func TestRemoveRatingRejectsUnidentifiedItemWithoutCallingMDBList(t *testing.T) {
	s := newTestServer(t, func(http.ResponseWriter, *http.Request) {
		t.Error("MDBList should not be called for items without IDs")
	})
	response := applyEvents(t, s, movieEvent("m1", removeRating, nil))
	assertStatus(t, response, "m1", statusRejected)
}

func TestSetRatingSplitsRequestsAtShowLimit(t *testing.T) {
	var showCounts []int
	s := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		var payload mdblistRatingsPayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Errorf("decode payload: %v", err)
		}
		showCounts = append(showCounts, len(payload.Shows))
		if len(showCounts) == 2 {
			// Only the second request's items share this rejection.
			writeJSON(w, `{"updated":{"shows":0},"not_found":{"shows":1}}`)
			return
		}
		writeJSON(w, `{"updated":{"shows":200}}`)
	})
	events := make([]*pluginv1.WatchSyncEvent, 0, maxRatingWriteEntries+1)
	for i := range maxRatingWriteEntries + 1 {
		event := seriesEvent(fmt.Sprintf("s%d", i), setRating, map[string]string{"tvdb": fmt.Sprint(1000 + i)})
		event.Rating = 6
		events = append(events, event)
	}
	response := applyEvents(t, s, events...)
	if !slices.Equal(showCounts, []int{maxRatingWriteEntries, 1}) {
		t.Fatalf("shows per request = %v, want [%d 1]", showCounts, maxRatingWriteEntries)
	}
	assertStatus(t, response, "s0", statusApplied)
	assertStatus(t, response, fmt.Sprintf("s%d", maxRatingWriteEntries-1), statusApplied)
	assertStatus(t, response, fmt.Sprintf("s%d", maxRatingWriteEntries), statusRetry)
}

func TestProviderRatingRoundsHalfUpAndClamps(t *testing.T) {
	cases := map[float64]int32{0: 0, 0.4: 0, 0.5: 1, 7.5: 8, 8.0: 8, 10: 10, 12: 10, -3: 1}
	for in, want := range cases {
		if got := providerRating(in); got != want {
			t.Errorf("providerRating(%v) = %d, want %d", in, got, want)
		}
	}
}

// writeRatings sets or removes the ratings of one movie (and, with count 2,
// one show) in a single request against a server that answers with body.
func writeRatings(t *testing.T, removing bool, body string, count int) *pluginv1.WatchSyncApplyEventsResponse {
	t.Helper()
	wantPath, operation := "/sync/ratings", setRating
	if removing {
		wantPath, operation = "/sync/ratings/remove", removeRating
	}
	s := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != wantPath {
			t.Errorf("path = %q, want %s", r.URL.Path, wantPath)
		}
		writeJSON(w, body)
	})
	events := []*pluginv1.WatchSyncEvent{
		movieEvent("m1", operation, map[string]string{"imdb": "tt0848228"}),
		seriesEvent("s1", operation, map[string]string{"tvdb": "81189"}),
	}[:count]
	for _, event := range events {
		event.Rating = 8
	}
	return applyEvents(t, s, events...)
}
