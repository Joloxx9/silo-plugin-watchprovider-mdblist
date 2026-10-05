package provider

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math"
	"strconv"
	"strings"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// MDBList rates on the integer 1 to 10 scale the plugin contract uses, so
// ratings pass through unchanged. Movie, show and episode ratings are read and
// written. Season ratings are only counted: the plugin contract has no season
// media type, so Silo cannot name one and does not rate one.

const (
	// maxRatingWriteEntries caps the titles in one rating write. MDBList
	// rejects a write that lists more than 200 shows with a 400; capping
	// movies, shows and episodes together keeps every request under that
	// limit.
	maxRatingWriteEntries = 200

	// seenHashSize is the size of one entry hash in a ratings page token.
	seenHashSize = 8

	ratingsChangedMessage = "MDBList ratings changed during the sync; the next sync reads them again"

	// The reasons a first page claims no snapshot, reported as sync warnings
	// with the wording the former built-in provider used.
	ratingsNoShowsWarning     = "mdblist returned no show ratings list; skipped rating removals"
	ratingsRepeatedWarning    = "mdblist ratings pages repeated an entry, so ratings changed during the read; skipped rating removals"
	ratingsOffsetPagedWarning = "mdblist ratings were read by offset, which can skip entries that change during the read; skipped rating removals"
	ratingsShortReadWarning   = "mdblist ratings read ended after %d of %d entries; skipped rating removals"
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

// mdblistRatedEpisode tolerates both shapes MDBList uses for episode rows:
// season and number inlined on the row, or nested under an `episode` object.
// The show is read from either level too, because an episode without its show
// cannot be addressed when its own IDs are missing.
type mdblistRatedEpisode struct {
	RatedAt time.Time
	Rating  float64
	Season  int
	Number  int
	Title   string
	IDs     mdblistIDs
	Show    mdblistShow
}

func (e *mdblistRatedEpisode) UnmarshalJSON(data []byte) error {
	var raw struct {
		RatedAt time.Time       `json:"rated_at"`
		Rating  float64         `json:"rating"`
		Season  int             `json:"season"`
		Number  int             `json:"number"`
		Title   string          `json:"title"`
		IDs     mdblistIDs      `json:"ids"`
		Show    mdblistShow     `json:"show"`
		Episode *mdblistEpisode `json:"episode"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	e.RatedAt = raw.RatedAt
	e.Rating = raw.Rating
	e.Season = raw.Season
	e.Number = raw.Number
	e.Title = raw.Title
	e.IDs = raw.IDs
	e.Show = raw.Show
	if raw.Episode != nil {
		if e.Season == 0 {
			e.Season = raw.Episode.Season
		}
		if e.Number == 0 {
			e.Number = raw.Episode.Number
		}
		if e.Title == "" {
			e.Title = raw.Episode.Title
			if e.Title == "" {
				e.Title = raw.Episode.Name
			}
		}
		if e.IDs == (mdblistIDs{}) {
			e.IDs = raw.Episode.IDs
		}
		if e.Show == (mdblistShow{}) {
			e.Show = raw.Episode.Show
		}
	}
	return nil
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
// there cannot say a show is unrated. Seasons are only counted, because the
// pagination counts them and Silo cannot name a season.
type mdblistRatingsResponse struct {
	Movies     []ratedEntry[mdblistRatedMovie]   `json:"movies"`
	Shows      json.RawMessage                   `json:"shows"`
	Seasons    []json.RawMessage                 `json:"seasons"`
	Episodes   []ratedEntry[mdblistRatedEpisode] `json:"episodes"`
	Pagination *mdblistRatingsPagination         `json:"pagination"`
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
	for _, entry := range payload.Episodes {
		episode := entry.value
		item, seenKey := ratingEpisodeRemoteState(episode, entry.raw)
		if item != nil {
			response.Items = append(response.Items, item)
		}
		see(seenKey)
	}
	// Seasons count toward total too, so a repeated one hides a skipped entry
	// just the same.
	for _, raw := range payload.Seasons {
		see("season entry " + string(raw))
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
	offsetPaged := !done && state.legacyOffset
	shortRead := done && total >= 0 && read < total
	unstable := repeated || offsetPaged || shortRead
	snapshot := token.Snapshot
	switch {
	case first:
		snapshot = hasShows && !unstable
		response.Warnings = ratingSnapshotWarnings(hasShows, repeated, offsetPaged, shortRead, read, total)
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

// ratingSnapshotWarnings explains why a first page claimed no snapshot.
func ratingSnapshotWarnings(hasShows, repeated, offsetPaged, shortRead bool, read, total int) []string {
	var warnings []string
	if !hasShows {
		warnings = append(warnings, ratingsNoShowsWarning)
	}
	if repeated {
		warnings = append(warnings, ratingsRepeatedWarning)
	}
	if offsetPaged {
		warnings = append(warnings, ratingsOffsetPagedWarning)
	}
	if shortRead {
		warnings = append(warnings, fmt.Sprintf(ratingsShortReadWarning, read, total))
	}
	return warnings
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

// ratingEpisodeRemoteState is ratingRemoteState for a rated episode. An
// episode is identified by its own IMDb, TMDB or TVDB ID when MDBList gives
// one, and otherwise by its show plus season and number, which is the form
// episodeKey produces and the one Silo stores against the connection.
//
// Silo matches an episode in its catalog by the episode's own external IDs, so
// a row carrying none is returned without them: Silo then skips it and leaves
// episodes out of the snapshot, exactly as an unidentified movie or show does.
func ratingEpisodeRemoteState(
	episode mdblistRatedEpisode,
	raw json.RawMessage,
) (*pluginv1.WatchSyncRemoteState, string) {
	entryKey := "episode entry " + string(raw)
	value := providerRating(episode.Rating)
	if value == 0 {
		return nil, entryKey
	}
	item := &pluginv1.WatchSyncRemoteState{
		Rating: &pluginv1.WatchSyncRemoteRatingState{Rating: value},
	}
	if !episode.RatedAt.IsZero() {
		item.Rating.RatedAt = timestamppb.New(episode.RatedAt)
	}
	key := episodeKey(episode.Show.IDs, episode.Season, episode.Number, episode.IDs)
	if episode.IDs.IMDb == "" && episode.IDs.TMDB <= 0 && episode.IDs.TVDB <= 0 {
		if key == "" {
			key = "unidentified:" + strconv.FormatUint(entryHash(entryKey), 16)
		}
		item.ProviderItemKey = key
		item.Media = episodeMedia(episode.Title, mdblistIDs{}, episode.Show, episode.Season, episode.Number)
		return item, entryKey
	}
	item.ProviderItemKey = key
	item.Media = episodeMedia(episode.Title, episode.IDs, episode.Show, episode.Season, episode.Number)
	return item, "episode " + key
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

// An episode rating is written inside its show, not as a title of its own:
// MDBList takes shows[].seasons[].episodes[], addressed by the show's IDs and
// the two numbers. A top-level "episodes" array is ignored without an error,
// which reads as a rating that was accepted and never appears.
type mdblistRatingEpisodeNested struct {
	Number  int    `json:"number"`
	Rating  int    `json:"rating,omitempty"`
	RatedAt string `json:"rated_at,omitempty"`
}

type mdblistRatingSeasonNested struct {
	Number   int                          `json:"number"`
	Episodes []mdblistRatingEpisodeNested `json:"episodes,omitempty"`
}

type mdblistRatingShowNested struct {
	IDs     mdblistIDs                  `json:"ids"`
	Seasons []mdblistRatingSeasonNested `json:"seasons,omitempty"`
}

// mdblistNestedRatingsPayload is the episode write. It reuses the "shows" key,
// so it goes in a request of its own rather than beside the flat show entries.
type mdblistNestedRatingsPayload struct {
	Shows []mdblistRatingShowNested `json:"shows"`
}

// episodeRatingTarget is one episode a write names, before grouping.
type episodeRatingTarget struct {
	showIDs mdblistIDs
	season  int
	number  int
	rating  int
	ratedAt string
}

// episodeRatingTargetFor resolves the show and position MDBList needs, or
// reports false when the event carries neither. The episode's own IDs are no
// use here: the write addresses it through its show.
func episodeRatingTargetFor(event *pluginv1.WatchSyncEvent) (episodeRatingTarget, bool) {
	media := event.GetMedia()
	target := episodeRatingTarget{
		showIDs: idsFromMedia(media.GetSeriesExternalIds()),
		season:  int(media.GetSeasonNumber()),
		number:  int(media.GetEpisodeNumber()),
	}
	if target.showIDs == (mdblistIDs{}) || target.number <= 0 {
		return target, false
	}
	return target, true
}

// nestEpisodeRatings groups targets under one entry per show and season, so a
// season rated episode by episode travels as one show entry.
func nestEpisodeRatings(targets []episodeRatingTarget) []mdblistRatingShowNested {
	var shows []mdblistRatingShowNested
	showAt := map[string]int{}
	seasonAt := map[string]int{}
	for _, t := range targets {
		key := showKey(t.showIDs)
		si, ok := showAt[key]
		if !ok {
			shows = append(shows, mdblistRatingShowNested{IDs: t.showIDs})
			si = len(shows) - 1
			showAt[key] = si
		}
		seasonKey := key + "|" + strconv.Itoa(t.season)
		ei, ok := seasonAt[seasonKey]
		if !ok {
			shows[si].Seasons = append(shows[si].Seasons, mdblistRatingSeasonNested{Number: t.season})
			ei = len(shows[si].Seasons) - 1
			seasonAt[seasonKey] = ei
		}
		shows[si].Seasons[ei].Episodes = append(shows[si].Seasons[ei].Episodes,
			mdblistRatingEpisodeNested{Number: t.number, Rating: t.rating, RatedAt: t.ratedAt})
	}
	return shows
}

// writeRatings sets or clears movie, show and episode ratings in requests of at most
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
	path := "/sync/ratings"
	if removing {
		path = "/sync/ratings/remove"
	}

	type titleWrite struct {
		event *pluginv1.WatchSyncEvent
		entry mdblistRatingEntry
	}
	type episodeWrite struct {
		event  *pluginv1.WatchSyncEvent
		target episodeRatingTarget
	}
	titles := make([]titleWrite, 0, len(events))
	episodes := make([]episodeWrite, 0, len(events))

	for _, event := range events {
		rating := 0
		ratedAt := ""
		if !removing {
			if event.GetRating() < 1 || event.GetRating() > 10 {
				results.reject(event, "MDBList ratings must be from 1 to 10")
				continue
			}
			rating = int(event.GetRating())
			if at := event.GetOccurredAt(); at != nil && at.CheckValid() == nil && !at.AsTime().IsZero() {
				ratedAt = at.AsTime().UTC().Format(time.RFC3339)
			}
		}
		if event.GetMedia().GetMediaType() == pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE {
			target, ok := episodeRatingTargetFor(event)
			if !ok {
				results.reject(event, "MDBList rating sync requires an episode's series external ID with a season and episode number")
				continue
			}
			target.rating, target.ratedAt = rating, ratedAt
			episodes = append(episodes, episodeWrite{event: event, target: target})
			continue
		}
		ids, ok := listItemIDs(event)
		if !ok {
			results.reject(event, "MDBList rating sync requires a movie or series with an external ID")
			continue
		}
		titles = append(titles, titleWrite{
			event: event,
			entry: mdblistRatingEntry{IDs: ids, Rating: rating, RatedAt: ratedAt},
		})
	}

	for chunk := range chunked(titles, maxRatingWriteEntries) {
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
		if fault := c.postRatingBatch(ctx, path, payload, sent, results, noun, removing); fault != nil {
			return fault
		}
	}

	// Episodes ride in their own request: they reuse the "shows" key, nested
	// under the season they belong to.
	for chunk := range chunked(episodes, maxRatingWriteEntries) {
		targets := make([]episodeRatingTarget, 0, len(chunk))
		sent := make([]*pluginv1.WatchSyncEvent, 0, len(chunk))
		for _, write := range chunk {
			targets = append(targets, write.target)
			sent = append(sent, write.event)
		}
		payload := mdblistNestedRatingsPayload{Shows: nestEpisodeRatings(targets)}
		if fault := c.postRatingBatch(ctx, path, payload, sent, results, noun, removing); fault != nil {
			return fault
		}
	}
	return nil
}

// chunked yields successive slices of at most size elements.
func chunked[T any](items []T, size int) func(func([]T) bool) {
	return func(yield func([]T) bool) {
		for start := 0; start < len(items); start += size {
			if !yield(items[start:min(start+size, len(items))]) {
				return
			}
		}
	}
}

// postRatingBatch sends one rating request and records the outcome every title
// in it shares.
//
// A response that reports counts decides first: MDBList ignores a payload shape
// it does not recognise without an error, and an "applied" on a request that
// changed nothing is worse than a retry, because Silo then records the rating
// as agreed and never sends it again.
func (c *apiClient) postRatingBatch(
	ctx context.Context,
	path string,
	payload any,
	sent []*pluginv1.WatchSyncEvent,
	results *resultSet,
	noun string,
	removing bool,
) *pluginv1.WatchSyncFault {
	if len(sent) == 0 {
		return nil
	}
	var response mdblistWriteResponse
	if err := c.post(ctx, path, payload, &response); err != nil {
		return failBatch(sent, results, faultFor(err))
	}
	reportedErrors := !emptyJSONValue(response.Errors)
	notFound := !emptyJSONValue(response.NotFound)
	counted, acted := response.counts()
	for _, event := range sent {
		switch {
		case reportedErrors:
			results.fail(event, temporaryFault("MDBList reported errors for the "+noun+" in the batch"))
		// Only a set is suspicious when nothing was recorded. A removal of a
		// rating MDBList no longer holds records nothing by definition, and the
		// not_found branch below already reconciles it as NO_CHANGE.
		case !removing && counted && !acted:
			results.fail(event, temporaryFault("MDBList accepted the request but recorded none of the "+noun+" in the batch"))
		case !notFound:
			results.set(event, pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED)
		case removing:
			results.set(event, pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_NO_CHANGE)
		default:
			results.fail(event, temporaryFault("MDBList did not accept one or more "+noun+" in the batch"))
		}
	}
	return nil
}
