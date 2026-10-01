package provider

import (
	"fmt"
	"strconv"
	"strings"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
)

// Provider item keys are stored by Silo against existing connections, so these
// functions must keep producing exactly the keys the former built-in MDBList
// provider produced.

func movieKey(ids mdblistIDs) string {
	switch {
	case ids.IMDb != "":
		return "imdb:" + ids.IMDb
	case ids.TMDB > 0:
		return "tmdb:" + strconv.Itoa(ids.TMDB)
	case ids.TVDB > 0:
		return "tvdb:" + strconv.Itoa(ids.TVDB)
	case ids.MDBList != "":
		return "mdblist:" + ids.MDBList
	default:
		return ""
	}
}

func showKey(ids mdblistIDs) string {
	switch {
	case ids.TVDB > 0:
		return "tvdb:" + strconv.Itoa(ids.TVDB)
	case ids.TMDB > 0:
		return "tmdb:" + strconv.Itoa(ids.TMDB)
	case ids.IMDb != "":
		return "imdb:" + ids.IMDb
	case ids.MDBList != "":
		return "mdblist:" + ids.MDBList
	default:
		return ""
	}
}

func episodeKey(showIDs mdblistIDs, season, episode int, episodeIDs mdblistIDs) string {
	switch {
	case episodeIDs.TVDB > 0:
		return "tvdb:" + strconv.Itoa(episodeIDs.TVDB)
	case episodeIDs.TMDB > 0:
		return "tmdb:" + strconv.Itoa(episodeIDs.TMDB)
	case episodeIDs.IMDb != "":
		return "imdb:" + episodeIDs.IMDb
	case showIDs.TVDB > 0:
		return fmt.Sprintf("show:tvdb:%d:s%d:e%d", showIDs.TVDB, season, episode)
	case showIDs.TMDB > 0:
		return fmt.Sprintf("show:tmdb:%d:s%d:e%d", showIDs.TMDB, season, episode)
	case showIDs.IMDb != "":
		return fmt.Sprintf("show:imdb:%s:s%d:e%d", showIDs.IMDb, season, episode)
	default:
		return ""
	}
}

// idsFromMedia reads the IMDb, TMDB, and TVDB IDs Silo sends for a title.
func idsFromMedia(ids map[string]string) mdblistIDs {
	return mdblistIDs{
		IMDb: strings.TrimSpace(ids["imdb"]),
		TMDB: parseInt(ids["tmdb"]),
		TVDB: parseInt(ids["tvdb"]),
	}
}

// idsFromProviderItemKey recovers the ID in a key this provider returned, for
// a list or rating event whose media carries no external ID.
func idsFromProviderItemKey(key string) mdblistIDs {
	prefix, value, ok := strings.Cut(strings.TrimSpace(key), ":")
	if !ok || value == "" {
		return mdblistIDs{}
	}
	switch prefix {
	case "imdb":
		return mdblistIDs{IMDb: value}
	case "tmdb":
		return mdblistIDs{TMDB: parseInt(value)}
	case "tvdb":
		return mdblistIDs{TVDB: parseInt(value)}
	case "mdblist":
		return mdblistIDs{MDBList: value}
	default:
		return mdblistIDs{}
	}
}

// listItemIDs returns the IDs that identify a movie or series in a watchlist
// or rating write: its external IDs, or else the ID in its provider item key.
// It reports false for any other media type and for an item with no ID.
func listItemIDs(event *pluginv1.WatchSyncEvent) (mdblistIDs, bool) {
	switch event.GetMedia().GetMediaType() {
	case pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_MOVIE,
		pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_SERIES:
	default:
		return mdblistIDs{}, false
	}
	ids := idsFromMedia(event.GetMedia().GetExternalIds())
	if ids == (mdblistIDs{}) {
		ids = idsFromProviderItemKey(event.GetProviderItemKey())
	}
	return ids, ids != (mdblistIDs{})
}

// externalIDs is the external_ids map of a remote title: the IMDb, TMDB, and
// TVDB IDs Silo matches its catalog by.
func externalIDs(ids mdblistIDs) map[string]string {
	out := map[string]string{}
	if ids.IMDb != "" {
		out["imdb"] = ids.IMDb
	}
	if ids.TMDB != 0 {
		out["tmdb"] = strconv.Itoa(ids.TMDB)
	}
	if ids.TVDB != 0 {
		out["tvdb"] = strconv.Itoa(ids.TVDB)
	}
	return out
}

func movieMedia(movie mdblistMovie) *pluginv1.WatchSyncMedia {
	return &pluginv1.WatchSyncMedia{
		MediaType:   pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_MOVIE,
		Title:       movie.Title,
		Year:        int32(movie.Year),
		ExternalIds: externalIDs(movie.IDs),
	}
}

func episodeMedia(title string, ids mdblistIDs, show mdblistShow, season, episode int) *pluginv1.WatchSyncMedia {
	return &pluginv1.WatchSyncMedia{
		MediaType:         pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE,
		Title:             title,
		ExternalIds:       externalIDs(ids),
		SeriesTitle:       show.Title,
		SeriesYear:        int32(show.Year),
		SeriesExternalIds: externalIDs(show.IDs),
		SeasonNumber:      int32(season),
		EpisodeNumber:     int32(episode),
	}
}

// titleMedia describes a movie or a series for watchlist and rating state.
func titleMedia(mediaType pluginv1.WatchSyncMediaType, title string, year int, ids mdblistIDs) *pluginv1.WatchSyncMedia {
	return &pluginv1.WatchSyncMedia{
		MediaType:   mediaType,
		Title:       title,
		Year:        int32(year),
		ExternalIds: externalIDs(ids),
	}
}

func parseInt(value string) int {
	parsed, _ := strconv.Atoi(strings.TrimSpace(value))
	return parsed
}
