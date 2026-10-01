package provider

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
)

const (
	// historyLookbackMargin widens the history read that checks a batch for
	// plays MDBList already holds. MDBList's since filter is documented only as
	// "items updated after", so the margin absorbs clock skew between Silo and
	// MDBList; the match itself compares exact seconds.
	historyLookbackMargin = 24 * time.Hour

	// maxHistoryCheckPages bounds that read. A recent batch fits in one page;
	// a backlog of old plays would otherwise cost a near-full history read per
	// batch against MDBList's daily quota, and MDBList's own folding of nearby
	// plays already keeps a write from adding a duplicate.
	maxHistoryCheckPages = 2

	unsupportedPlayMessage = "MDBList watched sync requires a movie or episode with an external ID"
)

// watchedPlay is one MARK_WATCHED or MARK_UNWATCHED event mapped to MDBList.
type watchedPlay struct {
	event   *pluginv1.WatchSyncEvent
	movie   *mdblistWatchedMoviePayload
	episode *mdblistWatchedEpisodePayload
	// watchedAt is the play time in whole UTC seconds, or zero when the event
	// has none.
	watchedAt time.Time
	tokens    []string
}

// watchedPlayFromEvent maps an event the way the built-in provider mapped a
// local play: a movie by its own IDs, an episode by its own IDs and its
// series' IDs with season and episode numbers. It reports false when MDBList
// could not identify the title.
func watchedPlayFromEvent(event *pluginv1.WatchSyncEvent, includeWatchedAt bool) (watchedPlay, bool) {
	media := event.GetMedia()
	play := watchedPlay{event: event}
	if occurred := event.GetOccurredAt(); occurred != nil && occurred.CheckValid() == nil && !occurred.AsTime().IsZero() {
		play.watchedAt = occurred.AsTime().UTC().Truncate(time.Second)
	}
	watchedAt := ""
	if includeWatchedAt && !play.watchedAt.IsZero() {
		watchedAt = play.watchedAt.Format(time.RFC3339)
	}
	ids := idsFromMedia(media.GetExternalIds())
	switch media.GetMediaType() {
	case pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_MOVIE:
		if ids == (mdblistIDs{}) {
			return watchedPlay{}, false
		}
		play.movie = &mdblistWatchedMoviePayload{IDs: ids, WatchedAt: watchedAt}
		play.tokens = movieTokens(ids, event.GetProviderItemKey())
	case pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE:
		showIDs := idsFromMedia(media.GetSeriesExternalIds())
		if ids == (mdblistIDs{}) && showIDs == (mdblistIDs{}) {
			return watchedPlay{}, false
		}
		season, number := int(media.GetSeasonNumber()), int(media.GetEpisodeNumber())
		play.episode = &mdblistWatchedEpisodePayload{IDs: ids, Season: season, Episode: number, WatchedAt: watchedAt}
		if showIDs != (mdblistIDs{}) {
			play.episode.Show = &mdblistRef{IDs: showIDs}
		}
		play.tokens = episodeTokens(ids, showIDs, season, number, event.GetProviderItemKey())
	default:
		return watchedPlay{}, false
	}
	return play, true
}

// watchedPlays maps events to plays, rejecting each event MDBList could not
// identify.
func watchedPlays(events []*pluginv1.WatchSyncEvent, results *resultSet, includeWatchedAt bool) []watchedPlay {
	plays := make([]watchedPlay, 0, len(events))
	for _, event := range events {
		play, ok := watchedPlayFromEvent(event, includeWatchedAt)
		if !ok {
			results.reject(event, unsupportedPlayMessage)
			continue
		}
		plays = append(plays, play)
	}
	return plays
}

func watchedPayload(plays []watchedPlay) mdblistWatchedPayload {
	var payload mdblistWatchedPayload
	for _, play := range plays {
		if play.movie != nil {
			payload.Movies = append(payload.Movies, *play.movie)
		} else {
			payload.Episodes = append(payload.Episodes, *play.episode)
		}
	}
	return payload
}

// markWatched adds plays to MDBList's history. Delivery is at least once, so
// it first reads the history around the batch's play times and answers a play
// MDBList already holds at the same second, the precision the built-in
// provider reconciled history at, with NO_CHANGE instead of writing it again.
// MDBList documents that it folds a written play into a nearby existing play
// instead of adding one, so a play the bounded read misses, or a batch whose
// read fails, is still written without creating a duplicate.
func (r *applyRun) markWatched(ctx context.Context, events []*pluginv1.WatchSyncEvent, results *resultSet) *pluginv1.WatchSyncFault {
	plays := watchedPlays(events, results, true)
	if len(plays) == 0 {
		return nil
	}
	held, fault := r.heldPlays(ctx, plays)
	if connectionWide(fault) {
		return fault
	}
	toSend := make([]watchedPlay, 0, len(plays))
	for _, play := range plays {
		if held.contains(play) {
			results.set(play.event, pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_NO_CHANGE)
			continue
		}
		toSend = append(toSend, play)
	}
	if len(toSend) == 0 {
		return nil
	}
	var response mdblistWriteResponse
	if err := r.client.post(ctx, "/sync/watched", watchedPayload(toSend), &response); err != nil {
		return failPlays(toSend, results, faultFor(err))
	}
	if !emptyJSONValue(response.NotFound) {
		// MDBList documents not_found only as an unstructured object, which
		// cannot show which play it rejected, so no play in the batch is
		// confirmed. The retry finds the plays that did land.
		return failPlays(toSend, results, temporaryFault("MDBList did not accept one or more watched items in the batch"))
	}
	for _, play := range toSend {
		results.set(play.event, pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED)
	}
	return nil
}

// markUnwatched clears the watched state of each title. A title MDBList does
// not hold as watched is already in the desired state.
func (c *apiClient) markUnwatched(ctx context.Context, events []*pluginv1.WatchSyncEvent, results *resultSet) *pluginv1.WatchSyncFault {
	plays := watchedPlays(events, results, false)
	if len(plays) == 0 {
		return nil
	}
	var response mdblistWriteResponse
	if err := c.post(ctx, "/sync/watched/remove", watchedPayload(plays), &response); err != nil {
		return failPlays(plays, results, faultFor(err))
	}
	status := pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED
	if !emptyJSONValue(response.NotFound) {
		status = pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_NO_CHANGE
	}
	for _, play := range plays {
		results.set(play.event, status)
	}
	return nil
}

func failPlays(plays []watchedPlay, results *resultSet, fault *pluginv1.WatchSyncFault) *pluginv1.WatchSyncFault {
	events := make([]*pluginv1.WatchSyncEvent, 0, len(plays))
	for _, play := range plays {
		events = append(events, play.event)
	}
	return failBatch(events, results, fault)
}

// playSet holds the identity tokens of plays MDBList holds, each at its play
// time in whole seconds.
type playSet map[string]struct{}

func playSetKey(token string, at time.Time) string {
	return token + "@" + strconv.FormatInt(at.Unix(), 10)
}

func (s playSet) add(tokens []string, at time.Time) {
	at = at.UTC().Truncate(time.Second)
	for _, token := range tokens {
		s[playSetKey(token, at)] = struct{}{}
	}
}

func (s playSet) contains(play watchedPlay) bool {
	if play.watchedAt.IsZero() {
		return false
	}
	for _, token := range play.tokens {
		if _, ok := s[playSetKey(token, play.watchedAt)]; ok {
			return true
		}
	}
	return false
}

// heldPlays reads MDBList's plays since the batch's earliest play time. The
// read stops after maxHistoryCheckPages pages, or when half the call's time box
// is spent; the plays read by then still count, and MDBList's own folding of
// nearby plays covers the rest. A failed read returns the plays read so far
// with the fault.
func (r *applyRun) heldPlays(ctx context.Context, plays []watchedPlay) (playSet, *pluginv1.WatchSyncFault) {
	held := playSet{}
	var earliest time.Time
	for _, play := range plays {
		if !play.watchedAt.IsZero() && (earliest.IsZero() || play.watchedAt.Before(earliest)) {
			earliest = play.watchedAt
		}
	}
	if earliest.IsZero() {
		return held, nil
	}
	since := earliest.Add(-historyLookbackMargin).Format(time.RFC3339)
	state := pageState{}
	for range maxHistoryCheckPages {
		if r.server.now().Sub(r.startedAt) >= r.budget/2 {
			return held, nil
		}
		// MDBList's cursors continue the result set they came from, and its
		// other cursor-paged reads refuse since next to cursor, so since goes
		// only on requests that start the set or page it by offset.
		extra := url.Values{"plays": {"all"}}
		if state.cursor == "" {
			extra.Set("since", since)
		}
		var payload mdblistWatchedResponse
		if err := r.client.get(ctx, "/sync/watched", state.query(extra), &payload); err != nil {
			return held, faultFor(err)
		}
		for _, movie := range payload.Movies {
			if movie.LastWatchedAt != nil {
				held.add(movieTokens(movie.Movie.IDs, movieKey(movie.Movie.IDs)), *movie.LastWatchedAt)
			}
		}
		for _, episode := range payload.Episodes {
			if episode.LastWatchedAt != nil {
				key := episodeKey(episode.Show.IDs, episode.Season, episode.Number, episode.IDs)
				held.add(episodeTokens(episode.IDs, episode.Show.IDs, episode.Season, episode.Number, key), *episode.LastWatchedAt)
			}
		}
		done, err := state.advance(payload.Pagination, payload.fetched())
		if err != nil {
			return held, temporaryFault(invalidPaginationPrefix + err.Error())
		}
		if done {
			return held, nil
		}
	}
	return held, nil
}

// movieTokens names a movie by each of its IDs and by its provider key, so a
// play matches whichever ID MDBList and Silo both know.
func movieTokens(ids mdblistIDs, providerItemKey string) []string {
	tokens := idTokens("movie|", ids)
	if key := strings.TrimSpace(providerItemKey); key != "" {
		tokens = append(tokens, "movie|"+key)
	}
	return tokens
}

// episodeTokens names an episode by each of its own IDs, by each of its
// series' IDs with season and episode numbers, and by its provider key. These
// are the forms episodeKey produces.
func episodeTokens(ids, showIDs mdblistIDs, season, episode int, providerItemKey string) []string {
	tokens := idTokens("episode|", ids)
	if showIDs.TVDB > 0 {
		tokens = append(tokens, fmt.Sprintf("episode|show:tvdb:%d:s%d:e%d", showIDs.TVDB, season, episode))
	}
	if showIDs.TMDB > 0 {
		tokens = append(tokens, fmt.Sprintf("episode|show:tmdb:%d:s%d:e%d", showIDs.TMDB, season, episode))
	}
	if showIDs.IMDb != "" {
		tokens = append(tokens, fmt.Sprintf("episode|show:imdb:%s:s%d:e%d", showIDs.IMDb, season, episode))
	}
	if key := strings.TrimSpace(providerItemKey); key != "" {
		tokens = append(tokens, "episode|"+key)
	}
	return tokens
}

func idTokens(prefix string, ids mdblistIDs) []string {
	var tokens []string
	if ids.IMDb != "" {
		tokens = append(tokens, prefix+"imdb:"+ids.IMDb)
	}
	if ids.TMDB > 0 {
		tokens = append(tokens, prefix+"tmdb:"+strconv.Itoa(ids.TMDB))
	}
	if ids.TVDB > 0 {
		tokens = append(tokens, prefix+"tvdb:"+strconv.Itoa(ids.TVDB))
	}
	return tokens
}
