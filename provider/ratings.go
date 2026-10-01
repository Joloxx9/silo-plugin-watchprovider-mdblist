package provider

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"hash/fnv"
	"math"
	"strconv"
	"strings"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// MDBList rates on the integer 1 to 10 scale the plugin contract uses, so
// ratings pass through unchanged. Only movie and show ratings are read and
// written: Silo rates movies and series, not seasons or episodes.

const (
	// maxRatingWriteEntries caps the titles in one rating write. MDBList
	// rejects a write that lists more than 200 shows with a 400; capping movies
	// and shows together keeps every request under that limit.
	maxRatingWriteEntries = 200

	// seenHashSize is the size of one entry hash in a ratings page token.
	seenHashSize = 8

	ratingsChangedMessage = "MDBList ratings changed during the sync; the next sync reads them again"
)

type mdblistRatedMovie struct {
	RatedAt time.Time `json:"rated_at"`
	// Rating is a float so that 8.0 decodes; null leaves it 0, which is unrated.
	Rating float64      `json:"rating"`
	Movie  mdblistMovie `json:"movie"`
}

type mdblistRatedShow struct {
	RatedAt time.Time   `json:"rated_at"`
	Rating  float64     `json:"rating"`
	Show    mdblistShow `json:"show"`
}

// ratedEntry is one rated title as decoded, kept with its raw JSON. The raw
// JSON identifies an entry that maps to no rating row when a read checks its
// pages for a repeated entry.
type ratedEntry[T any] struct {
	value T
	raw   json.RawMessage
}

func (e *ratedEntry[T]) UnmarshalJSON(data []byte) error {
	if err := json.Unmarshal(data, &e.value); err != nil {
		return err
	}
	e.raw = append(json.RawMessage(nil), data...)
	return nil
}

// mdblistRatingsResponse is one page of GET /sync/ratings. Shows stays raw so
// the read can tell a missing shows list from an empty one: a list that is not
// there cannot say a show is unrated. Seasons and episodes are only counted,
// because the pagination counts them too.
type mdblistRatingsResponse struct {
	Movies     []ratedEntry[mdblistRatedMovie] `json:"movies"`
	Shows      json.RawMessage                 `json:"shows"`
	Seasons    []json.RawMessage               `json:"seasons"`
	Episodes   []json.RawMessage               `json:"episodes"`
	Pagination *mdblistRatingsPagination       `json:"pagination"`
}

// mdblistRatingsPagination is the pagination of a ratings page. The schema
// documents {total, limit, offset, next_cursor}, where total counts the
// entries of the whole read across movies, shows, seasons, and episodes. An
// older sample reports per-type totals with has_more instead.
type mdblistRatingsPagination struct {
	mdblistPagination
	Total         *int `json:"total"`
	TotalMovies   *int `json:"total_movies"`
	TotalShows    *int `json:"total_shows"`
	TotalSeasons  *int `json:"total_seasons"`
	TotalEpisodes *int `json:"total_episodes"`
}

// entryTotal returns the number of entries in the whole read: total, or else
// the sum of the per-type totals. It reports false when the page has neither.
func (p *mdblistRatingsPagination) entryTotal() (int, bool) {
	if p == nil {
		return 0, false
	}
	if p.Total != nil {
		return *p.Total, true
	}
	sum, found := 0, false
	for _, n := range []*int{p.TotalMovies, p.TotalShows, p.TotalSeasons, p.TotalEpisodes} {
		if n != nil {
			sum += *n
			found = true
		}
	}
	return sum, found
}

func (p *mdblistRatingsPagination) base() *mdblistPagination {
	if p == nil {
		return nil
	}
	return &p.mdblistPagination
}

// advanceTo is advance for a read that knows its entry total, or -1. While
// fewer than total entries are read, a page without next_cursor does not end
// the read: it goes on by offset until a page comes back empty, and the
// caller checks whether the read reached total. The offset is always the
// count of entries read so far.
func (s *pageState) advanceTo(pagination *mdblistPagination, fetched, read, total int) (bool, error) {
	hasCursor := pagination != nil && strings.TrimSpace(pagination.NextCursor) != ""
	if total < 0 || read >= total || hasCursor {
		done, err := s.advance(pagination, fetched)
		if s.legacyOffset {
			s.offset = read
		}
		return done, err
	}
	if fetched == 0 {
		return true, nil
	}
	s.cursor = ""
	s.legacyOffset = true
	s.offset = read
	return false, nil
}

// ratedShows decodes a shows list and reports whether the response carried
// one. A null list counts as missing.
func ratedShows(raw json.RawMessage) ([]ratedEntry[mdblistRatedShow], bool, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return nil, false, nil
	}
	var shows []ratedEntry[mdblistRatedShow]
	if err := json.Unmarshal(raw, &shows); err != nil {
		return nil, false, err
	}
	return shows, true, nil
}

// listRatings returns one page of movie and show ratings. Silo can treat a
// complete snapshot's absent titles as unrated, so the plugin claims one only
// for a read the former built-in provider trusted, and it must decide on the
// first page:
//   - a read that goes on by offset is never a snapshot: offsets shift when
//     ratings change mid-read, and two changes can skip an entry without
//     repeating one or changing the count;
//   - a first page without a shows list is not one, because a list that is
//     not there cannot say a show is unrated;
//   - a page that repeats an entry, or a read that ends short of the entry
//     total a page reported, means ratings changed mid-read: a first page that
//     shows it claims no snapshot, and a later page fails the traversal so the
//     next sync reads every rating again.
//
// A rated title with only MDBList's own IDs could be any local title. It is
// returned without external IDs, which makes Silo leave its kind out of the
// snapshot, as the built-in provider did.
func listRatings(ctx context.Context, client *apiClient, rawToken string) *pluginv1.WatchSyncListRemoteStateResponse {
	token, fault := decodePageToken(rawToken, tokenKindRatings)
	if fault != nil {
		return listFault(fault)
	}
	first := strings.TrimSpace(rawToken) == ""
	state := token.state()
	var payload mdblistRatingsResponse
	if err := client.get(ctx, "/sync/ratings", state.query(nil), &payload); err != nil {
		return listFault(faultFor(err))
	}
	shows, hasShows, err := ratedShows(payload.Shows)
	if err != nil {
		return listFault(temporaryFault("MDBList returned an unreadable response"))
	}

	seen := decodeSeenSet(token.Seen)
	repeated := false
	see := func(key string) {
		if !seen.add(key) {
			repeated = true
		}
	}
	response := &pluginv1.WatchSyncListRemoteStateResponse{}
	for _, entry := range payload.Movies {
		movie := entry.value
		item, seenKey := ratingRemoteState(pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_MOVIE,
			movie.Rating, movie.RatedAt, movie.Movie.Title, movie.Movie.Year, movie.Movie.IDs, entry.raw)
		if item != nil {
			response.Items = append(response.Items, item)
		}
		see(seenKey)
	}
	for _, entry := range shows {
		show := entry.value
		item, seenKey := ratingRemoteState(pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_SERIES,
			show.Rating, show.RatedAt, show.Show.Title, show.Show.Year, show.Show.IDs, entry.raw)
		if item != nil {
			response.Items = append(response.Items, item)
		}
		see(seenKey)
	}
	// Seasons and episodes count toward total too, so a repeated one hides a
	// skipped entry just the same.
	for _, raw := range payload.Seasons {
		see("season entry " + string(raw))
	}
	for _, raw := range payload.Episodes {
		see("episode entry " + string(raw))
	}

	fetched := len(payload.Movies) + len(shows) + len(payload.Seasons) + len(payload.Episodes)
	read := token.Read + fetched
	total := -1
	if token.Total != nil {
		total = *token.Total
	}
	if n, ok := payload.Pagination.entryTotal(); ok {
		total = max(total, n)
	}
	done, err := state.advanceTo(payload.Pagination.base(), fetched, read, total)
	if err != nil {
		return listFault(temporaryFault(invalidPaginationPrefix + err.Error()))
	}
	unstable := repeated || (!done && state.legacyOffset) || (done && total >= 0 && read < total)
	snapshot := token.Snapshot
	switch {
	case first:
		snapshot = hasShows && !unstable
	case snapshot && unstable:
		return listFault(temporaryFault(ratingsChangedMessage))
	}
	response.CompleteSnapshot = snapshot
	if !done {
		token.setState(state)
		token.Read = read
		token.Total = nil
		if total >= 0 {
			token.Total = &total
		}
		token.Snapshot = snapshot
		token.Seen = nil
		if snapshot {
			token.Seen = seen.encode()
		}
		response.NextPageToken = encodePageToken(token)
	}
	return response
}

// ratingRemoteState maps one rated title, and returns the key the read uses
// to spot a repeated entry. It returns no state for an unrated title (a null
// or zero rating).
func ratingRemoteState(
	mediaType pluginv1.WatchSyncMediaType,
	rating float64,
	ratedAt time.Time,
	title string,
	year int,
	ids mdblistIDs,
	raw json.RawMessage,
) (*pluginv1.WatchSyncRemoteState, string) {
	entryKey := "movie entry " + string(raw)
	if mediaType == pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_SERIES {
		entryKey = "show entry " + string(raw)
	}
	value := providerRating(rating)
	if value == 0 {
		return nil, entryKey
	}
	item := &pluginv1.WatchSyncRemoteState{
		Rating: &pluginv1.WatchSyncRemoteRatingState{Rating: value},
	}
	if !ratedAt.IsZero() {
		item.Rating.RatedAt = timestamppb.New(ratedAt)
	}
	key := movieKey(ids)
	if mediaType == pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_SERIES {
		key = showKey(ids)
	}
	if ids.IMDb == "" && ids.TMDB <= 0 && ids.TVDB <= 0 {
		// Silo matches its catalog by IMDb, TMDB, or TVDB ID, so this title
		// carries none: Silo then skips it and leaves its kind out of the
		// snapshot. Its key only has to be present.
		if key == "" {
			key = "unidentified:" + strconv.FormatUint(entryHash(entryKey), 16)
		}
		item.ProviderItemKey = key
		item.Media = titleMedia(mediaType, title, year, mdblistIDs{})
		return item, entryKey
	}
	item.ProviderItemKey = key
	item.Media = titleMedia(mediaType, title, year, ids)
	if mediaType == pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_SERIES {
		return item, "series " + key
	}
	return item, "movie " + key
}

// providerRating rounds a rating half up to the 1 to 10 scale and clamps it;
// 0 means unrated.
func providerRating(rating float64) int32 {
	value := math.Round(rating)
	if value == 0 || math.IsNaN(value) {
		return 0
	}
	return int32(min(max(value, 1), 10))
}

// seenSet holds 64-bit hashes of the entries a ratings read has returned, so
// a later page can spot a repeat. A page token carries them between pages.
type seenSet map[uint64]struct{}

func decodeSeenSet(encoded []byte) seenSet {
	set := make(seenSet, len(encoded)/seenHashSize)
	for start := 0; start+seenHashSize <= len(encoded); start += seenHashSize {
		set[binary.LittleEndian.Uint64(encoded[start:])] = struct{}{}
	}
	return set
}

// add records key and reports false when the set already held it.
func (s seenSet) add(key string) bool {
	hash := entryHash(key)
	if _, ok := s[hash]; ok {
		return false
	}
	s[hash] = struct{}{}
	return true
}

func (s seenSet) encode() []byte {
	out := make([]byte, 0, len(s)*seenHashSize)
	for hash := range s {
		out = binary.LittleEndian.AppendUint64(out, hash)
	}
	return out
}

func entryHash(key string) uint64 {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(key))
	return hash.Sum64()
}

type mdblistRatingEntry struct {
	IDs     mdblistIDs `json:"ids"`
	Rating  int        `json:"rating,omitempty"`
	RatedAt string     `json:"rated_at,omitempty"`
}

type mdblistRatingsPayload struct {
	Movies []mdblistRatingEntry `json:"movies,omitempty"`
	Shows  []mdblistRatingEntry `json:"shows,omitempty"`
}

// writeRatings sets or clears movie and show ratings in requests of at most
// maxRatingWriteEntries titles. MDBList replaces an existing rating, so
// resending one is harmless. Every title of one request shares its outcome:
//   - a request that reports errors is retried, set or removal;
//   - a set with any not_found entry is retried, because no single title can
//     be shown to have been accepted;
//   - a removal with any not_found entry leaves every title unrated, which is
//     the desired state;
//   - a title MDBList cannot identify is left out of the request and rejected.
func (c *apiClient) writeRatings(ctx context.Context, events []*pluginv1.WatchSyncEvent, results *resultSet, removing bool) *pluginv1.WatchSyncFault {
	noun := "ratings"
	if removing {
		noun = "rating removals"
	}
	type ratingWrite struct {
		event *pluginv1.WatchSyncEvent
		entry mdblistRatingEntry
	}
	writes := make([]ratingWrite, 0, len(events))
	for _, event := range events {
		ids, ok := listItemIDs(event)
		if !ok {
			results.reject(event, "MDBList rating sync requires a movie or series with an external ID")
			continue
		}
		entry := mdblistRatingEntry{IDs: ids}
		if !removing {
			if event.GetRating() < 1 || event.GetRating() > 10 {
				results.reject(event, "MDBList ratings must be from 1 to 10")
				continue
			}
			entry.Rating = int(event.GetRating())
			if ratedAt := event.GetOccurredAt(); ratedAt != nil && ratedAt.CheckValid() == nil && !ratedAt.AsTime().IsZero() {
				entry.RatedAt = ratedAt.AsTime().UTC().Format(time.RFC3339)
			}
		}
		writes = append(writes, ratingWrite{event: event, entry: entry})
	}
	path := "/sync/ratings"
	if removing {
		path = "/sync/ratings/remove"
	}
	for start := 0; start < len(writes); start += maxRatingWriteEntries {
		chunk := writes[start:min(start+maxRatingWriteEntries, len(writes))]
		var payload mdblistRatingsPayload
		sent := make([]*pluginv1.WatchSyncEvent, 0, len(chunk))
		for _, write := range chunk {
			if write.event.GetMedia().GetMediaType() == pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_MOVIE {
				payload.Movies = append(payload.Movies, write.entry)
			} else {
				payload.Shows = append(payload.Shows, write.entry)
			}
			sent = append(sent, write.event)
		}
		var response mdblistWriteResponse
		if err := c.post(ctx, path, payload, &response); err != nil {
			if fault := failBatch(sent, results, faultFor(err)); fault != nil {
				return fault
			}
			continue
		}
		reportedErrors := !emptyJSONValue(response.Errors)
		notFound := !emptyJSONValue(response.NotFound)
		for _, event := range sent {
			switch {
			case reportedErrors:
				results.fail(event, temporaryFault("MDBList reported errors for the "+noun+" in the batch"))
			case !notFound:
				results.set(event, pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED)
			case removing:
				results.set(event, pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_NO_CHANGE)
			default:
				results.fail(event, temporaryFault("MDBList did not accept one or more "+noun+" in the batch"))
			}
		}
	}
	return nil
}
