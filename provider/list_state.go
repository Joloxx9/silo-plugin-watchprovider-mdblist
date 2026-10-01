package provider

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/url"
	"strconv"
	"strings"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// syncPageLimit is the page size of every MDBList sync read, the largest
// MDBList allows with cursor pagination. A host page carries one MDBList page,
// which can hold more items than the host's page_size: reading smaller pages
// would multiply requests against MDBList's daily quota, and a thousand items
// stay far below the plugin RPC's response size limit.
const syncPageLimit = 1000

const (
	tokenKindWatched   = "watched"
	tokenKindWatchlist = "watchlist"
	tokenKindRatings   = "ratings"

	invalidPageTokenMessage = "MDBList page token is invalid"
	invalidPaginationPrefix = "MDBList returned invalid pagination: "
)

// pageState is the position of a paginated MDBList read: a cursor, or an
// offset for responses that carry no cursor.
type pageState struct {
	cursor       string
	offset       int
	legacyOffset bool
}

func (s pageState) query(extra url.Values) url.Values {
	query := url.Values{"limit": {strconv.Itoa(syncPageLimit)}}
	for key, values := range extra {
		query[key] = values
	}
	if s.cursor != "" {
		query.Set("cursor", s.cursor)
	} else if s.legacyOffset {
		query.Set("offset", strconv.Itoa(s.offset))
	}
	return query
}

// advance moves past a page that held fetched entries and reports whether the
// read is done. It prefers next_cursor, and keeps the deprecated offset
// fallback for responses without pagination metadata or with has_more only.
func (s *pageState) advance(pagination *mdblistPagination, fetched int) (bool, error) {
	if pagination != nil {
		next := strings.TrimSpace(pagination.NextCursor)
		if next != "" {
			if next == s.cursor {
				return false, errors.New("provider returned the same cursor twice")
			}
			s.cursor = next
			return false, nil
		}
		if !pagination.HasMore {
			return true, nil
		}
		if fetched == 0 {
			return false, errors.New("provider reported more pages without returning items")
		}
		s.cursor = ""
		s.legacyOffset = true
		s.offset += fetched
		return false, nil
	}
	if fetched < syncPageLimit {
		return true, nil
	}
	s.cursor = ""
	s.legacyOffset = true
	s.offset += fetched
	return false, nil
}

// pageToken is the opaque continuation of one traversal. Plugins keep no
// state between calls, so everything the next page needs travels here.
type pageToken struct {
	Kind   string `json:"k"`
	Cursor string `json:"c,omitempty"`
	Offset int    `json:"o,omitempty"`
	Legacy bool   `json:"l,omitempty"`

	// Ratings only: entries read so far, the largest entry total a page
	// reported, whether the traversal claims a complete snapshot, and the
	// hashes of every entry read so far.
	Read     int    `json:"r,omitempty"`
	Total    *int   `json:"t,omitempty"`
	Snapshot bool   `json:"s,omitempty"`
	Seen     []byte `json:"h,omitempty"`
}

func (t pageToken) state() pageState {
	return pageState{cursor: t.Cursor, offset: t.Offset, legacyOffset: t.Legacy}
}

func (t *pageToken) setState(state pageState) {
	t.Cursor, t.Offset, t.Legacy = state.cursor, state.offset, state.legacyOffset
}

func decodePageToken(raw, kind string) (pageToken, *pluginv1.WatchSyncFault) {
	if strings.TrimSpace(raw) == "" {
		return pageToken{Kind: kind}, nil
	}
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return pageToken{}, invalidRequestFault(invalidPageTokenMessage)
	}
	var token pageToken
	if err := json.Unmarshal(decoded, &token); err != nil || token.Kind != kind ||
		token.Offset < 0 || token.Read < 0 || len(token.Seen)%seenHashSize != 0 ||
		(token.Cursor == "" && !token.Legacy) {
		return pageToken{}, invalidRequestFault(invalidPageTokenMessage)
	}
	return token, nil
}

func encodePageToken(token pageToken) string {
	encoded, _ := json.Marshal(token)
	return base64.RawURLEncoding.EncodeToString(encoded)
}

func listFault(fault *pluginv1.WatchSyncFault) *pluginv1.WatchSyncListRemoteStateResponse {
	return &pluginv1.WatchSyncListRemoteStateResponse{Fault: fault}
}

// listWatched returns one page of play history. MDBList's plays=all lists
// one row per movie or episode play; the shows and seasons arrays are rollups
// of episode state, not proof that every episode in them was watched, so only
// the playable leaves are imported. Every traversal reads the whole history.
func listWatched(ctx context.Context, client *apiClient, rawToken string) *pluginv1.WatchSyncListRemoteStateResponse {
	token, fault := decodePageToken(rawToken, tokenKindWatched)
	if fault != nil {
		return listFault(fault)
	}
	state := token.state()
	var payload mdblistWatchedResponse
	if err := client.get(ctx, "/sync/watched", state.query(url.Values{"plays": {"all"}}), &payload); err != nil {
		return listFault(faultFor(err))
	}
	response := &pluginv1.WatchSyncListRemoteStateResponse{
		Items:            watchedStates(payload),
		CompleteSnapshot: true,
	}
	done, err := state.advance(payload.Pagination, payload.fetched())
	if err != nil {
		return listFault(temporaryFault(invalidPaginationPrefix + err.Error()))
	}
	if !done {
		token.setState(state)
		response.NextPageToken = encodePageToken(token)
	}
	return response
}

func watchedStates(payload mdblistWatchedResponse) []*pluginv1.WatchSyncRemoteState {
	items := make([]*pluginv1.WatchSyncRemoteState, 0, len(payload.Movies)+len(payload.Episodes))
	for _, movie := range payload.Movies {
		key := movieKey(movie.Movie.IDs)
		if movie.LastWatchedAt == nil || key == "" {
			continue
		}
		items = append(items, &pluginv1.WatchSyncRemoteState{
			ProviderItemKey: key,
			Media:           movieMedia(movie.Movie),
			Watched:         watchedState(*movie.LastWatchedAt),
		})
	}
	for _, episode := range payload.Episodes {
		key := episodeKey(episode.Show.IDs, episode.Season, episode.Number, episode.IDs)
		if episode.LastWatchedAt == nil || key == "" {
			continue
		}
		items = append(items, &pluginv1.WatchSyncRemoteState{
			ProviderItemKey: key,
			Media:           episodeMedia(episode.Title, episode.IDs, episode.Show, episode.Season, episode.Number),
			Watched:         watchedState(*episode.LastWatchedAt),
		})
	}
	return items
}

func watchedState(watchedAt time.Time) *pluginv1.WatchSyncRemoteWatchedState {
	return &pluginv1.WatchSyncRemoteWatchedState{PlayCount: 1, LastWatchedAt: timestamppb.New(watchedAt)}
}

// listProgress returns every paused playback session in one page: MDBList
// lists them in one unpaginated response.
func (s *Server) listProgress(ctx context.Context, client *apiClient, rawToken string) *pluginv1.WatchSyncListRemoteStateResponse {
	if strings.TrimSpace(rawToken) != "" {
		return listFault(invalidRequestFault(invalidPageTokenMessage))
	}
	var payload mdblistPlaybackResponse
	if err := client.get(ctx, "/sync/playback", nil, &payload); err != nil {
		return listFault(faultFor(err))
	}
	response := &pluginv1.WatchSyncListRemoteStateResponse{CompleteSnapshot: true}
	now := s.now().UTC()
	for _, item := range payload.items() {
		// Skip actively scrobbling sessions: they are mid-playback on another
		// device and would race the local session if imported as resume points.
		if strings.EqualFold(item.Action, "start") {
			continue
		}
		if state := progressState(item, now); state != nil {
			response.Items = append(response.Items, state)
		}
	}
	return response
}

func progressState(item mdblistPlaybackItem, now time.Time) *pluginv1.WatchSyncRemoteState {
	percent := float64(item.Progress)
	if percent < 0 {
		return nil
	}
	pausedAt := item.PausedAt
	if pausedAt.IsZero() {
		pausedAt = now
	}
	state := &pluginv1.WatchSyncRemoteState{
		Progress: &pluginv1.WatchSyncRemoteProgressState{
			// Silo reads resume progress in [0, 100); a finished session
			// resumes at its very end.
			ProgressPercent: min(percent, 99.999),
			PausedAt:        timestamppb.New(pausedAt),
		},
	}
	if episode := item.Episode; episode != nil {
		state.ProviderItemKey = episodeKey(item.Show.IDs, episode.Season, episode.Number, episode.IDs)
		state.Media = episodeMedia(episode.Title, episode.IDs, item.Show, episode.Season, episode.Number)
	} else {
		state.ProviderItemKey = movieKey(item.Movie.IDs)
		state.Media = movieMedia(item.Movie)
	}
	if state.ProviderItemKey == "" {
		return nil
	}
	return state
}

// listWatchlist returns one page of the watchlist in MDBList's order. Silo
// mirrors that order, so every traversal is a complete snapshot.
func listWatchlist(ctx context.Context, client *apiClient, rawToken string) *pluginv1.WatchSyncListRemoteStateResponse {
	token, fault := decodePageToken(rawToken, tokenKindWatchlist)
	if fault != nil {
		return listFault(fault)
	}
	state := token.state()
	var payload mdblistWatchlistResponse
	if err := client.get(ctx, "/watchlist/items", state.query(nil), &payload); err != nil {
		return listFault(faultFor(err))
	}
	response := &pluginv1.WatchSyncListRemoteStateResponse{
		Items:            watchlistStates(payload),
		CompleteSnapshot: true,
	}
	done, err := state.advance(payload.Pagination, len(payload.Movies)+len(payload.Shows))
	if err != nil {
		return listFault(temporaryFault(invalidPaginationPrefix + err.Error()))
	}
	if !done {
		token.setState(state)
		response.NextPageToken = encodePageToken(token)
	}
	return response
}

// watchlistStates leaves listed_at unset: MDBList's watchlist reports no add
// time, and Silo stamps the import time instead.
func watchlistStates(payload mdblistWatchlistResponse) []*pluginv1.WatchSyncRemoteState {
	items := make([]*pluginv1.WatchSyncRemoteState, 0, len(payload.Movies)+len(payload.Shows))
	for _, item := range payload.Movies {
		ids := item.preferredIDs()
		if key := movieKey(ids); key != "" {
			items = append(items, &pluginv1.WatchSyncRemoteState{
				ProviderItemKey: key,
				Media:           titleMedia(pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_MOVIE, item.Title, item.ReleaseYear, ids),
				Watchlist:       &pluginv1.WatchSyncRemoteListState{},
			})
		}
	}
	for _, item := range payload.Shows {
		ids := item.preferredIDs()
		if key := showKey(ids); key != "" {
			items = append(items, &pluginv1.WatchSyncRemoteState{
				ProviderItemKey: key,
				Media:           titleMedia(pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_SERIES, item.Title, item.ReleaseYear, ids),
				Watchlist:       &pluginv1.WatchSyncRemoteListState{},
			})
		}
	}
	return items
}
