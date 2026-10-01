package provider

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type mdblistIDs struct {
	IMDb    string `json:"imdb,omitempty"`
	TMDB    int    `json:"tmdb,omitempty"`
	TVDB    int    `json:"tvdb,omitempty"`
	Trakt   int    `json:"trakt,omitempty"`
	MDBList string `json:"mdblist,omitempty"`
}

// UnmarshalJSON accepts numeric IDs as numbers or strings; MDBList uses both.
func (ids *mdblistIDs) UnmarshalJSON(data []byte) error {
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	ids.IMDb = stringFromJSON(raw["imdb"])
	ids.TMDB = intFromJSON(raw["tmdb"])
	ids.TVDB = intFromJSON(raw["tvdb"])
	ids.Trakt = intFromJSON(raw["trakt"])
	ids.MDBList = stringFromJSON(raw["mdblist"])
	return nil
}

type mdblistMovie struct {
	Title string     `json:"title"`
	Year  int        `json:"year"`
	IDs   mdblistIDs `json:"ids"`
}

type mdblistShow struct {
	Title string     `json:"title"`
	Year  int        `json:"year"`
	IDs   mdblistIDs `json:"ids"`
}

type mdblistEpisode struct {
	Title  string      `json:"title"`
	Name   string      `json:"name"`
	Season int         `json:"season"`
	Number int         `json:"number"`
	IDs    mdblistIDs  `json:"ids"`
	Show   mdblistShow `json:"show"`
}

type mdblistWatchedMovie struct {
	LastWatchedAt *time.Time
	Movie         mdblistMovie
}

func (m *mdblistWatchedMovie) UnmarshalJSON(data []byte) error {
	var raw struct {
		LastWatchedAt *time.Time   `json:"last_watched_at"`
		WatchedAt     *time.Time   `json:"watched_at"`
		Movie         mdblistMovie `json:"movie"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	m.LastWatchedAt = firstTimestamp(raw.LastWatchedAt, raw.WatchedAt)
	m.Movie = raw.Movie
	return nil
}

// mdblistWatchedEpisode tolerates both shapes MDBList uses for episode rows:
// season/number inlined on the row, or nested under an `episode` object.
type mdblistWatchedEpisode struct {
	LastWatchedAt *time.Time
	Season        int
	Number        int
	Title         string
	IDs           mdblistIDs
	Show          mdblistShow
}

func (e *mdblistWatchedEpisode) UnmarshalJSON(data []byte) error {
	var raw struct {
		LastWatchedAt *time.Time      `json:"last_watched_at"`
		WatchedAt     *time.Time      `json:"watched_at"`
		Season        int             `json:"season"`
		Number        int             `json:"number"`
		Title         string          `json:"title"`
		IDs           mdblistIDs      `json:"ids"`
		Show          mdblistShow     `json:"show"`
		Episode       *mdblistEpisode `json:"episode"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	e.LastWatchedAt = firstTimestamp(raw.LastWatchedAt, raw.WatchedAt)
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

func firstTimestamp(preferred, fallback *time.Time) *time.Time {
	if preferred != nil {
		return preferred
	}
	return fallback
}

// mdblistWatchedResponse is one page of GET /sync/watched. Shows and seasons
// are only counted: they are rollups of episode state, and the offset fallback
// counts them too.
type mdblistWatchedResponse struct {
	Movies     []mdblistWatchedMovie   `json:"movies"`
	Shows      []json.RawMessage       `json:"shows"`
	Seasons    []json.RawMessage       `json:"seasons"`
	Episodes   []mdblistWatchedEpisode `json:"episodes"`
	Pagination *mdblistPagination      `json:"pagination"`
}

func (r mdblistWatchedResponse) fetched() int {
	return len(r.Movies) + len(r.Shows) + len(r.Seasons) + len(r.Episodes)
}

type mdblistPagination struct {
	NextCursor string `json:"next_cursor"`
	HasMore    bool   `json:"has_more"`
}

type mdblistPlaybackItem struct {
	Progress mdblistProgress `json:"progress"`
	PausedAt time.Time       `json:"paused_at"`
	Type     string          `json:"type"`
	Action   string          `json:"action"`
	Movie    mdblistMovie    `json:"movie"`
	Show     mdblistShow     `json:"show"`
	Episode  *mdblistEpisode `json:"episode"`
}

type mdblistProgress float64

func (p *mdblistProgress) UnmarshalJSON(data []byte) error {
	trimmed := strings.TrimSpace(string(data))
	if trimmed == "" || trimmed == "null" {
		*p = 0
		return nil
	}
	var value float64
	if err := json.Unmarshal(data, &value); err == nil {
		*p = mdblistProgress(value)
		return nil
	}
	var text string
	if err := json.Unmarshal(data, &text); err != nil {
		return errors.New("mdblist progress must be a number or numeric string")
	}
	text = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(text), "%"))
	if text == "" {
		*p = 0
		return nil
	}
	value, err := strconv.ParseFloat(text, 64)
	if err != nil {
		return fmt.Errorf("parse mdblist progress: %w", err)
	}
	*p = mdblistProgress(value)
	return nil
}

// mdblistPlaybackResponse accepts both the live API shape (a flat array of
// playback items) and the older documented shape ({paused, scrobbling}
// arrays).
type mdblistPlaybackResponse struct {
	flat   []mdblistPlaybackItem
	nested struct {
		Paused     []mdblistPlaybackItem `json:"paused"`
		Scrobbling []mdblistPlaybackItem `json:"scrobbling"`
	}
}

func (r *mdblistPlaybackResponse) UnmarshalJSON(data []byte) error {
	trimmed := bytes.TrimLeft(data, " \t\r\n")
	if len(trimmed) > 0 && trimmed[0] == '[' {
		return json.Unmarshal(data, &r.flat)
	}
	return json.Unmarshal(data, &r.nested)
}

func (r mdblistPlaybackResponse) items() []mdblistPlaybackItem {
	if len(r.flat) > 0 {
		return r.flat
	}
	out := make([]mdblistPlaybackItem, 0, len(r.nested.Paused)+len(r.nested.Scrobbling))
	out = append(out, r.nested.Paused...)
	for _, item := range r.nested.Scrobbling {
		if item.Action == "" {
			item.Action = "start"
		}
		out = append(out, item)
	}
	return out
}

type mdblistWatchlistItem struct {
	ID          int        `json:"id"`
	Title       string     `json:"title"`
	IMDbID      string     `json:"imdb_id"`
	TVDBID      int        `json:"tvdb_id"`
	Mediatype   string     `json:"mediatype"`
	ReleaseYear int        `json:"release_year"`
	IDs         mdblistIDs `json:"ids"`
}

func (i mdblistWatchlistItem) preferredIDs() mdblistIDs {
	ids := i.IDs
	if ids.IMDb == "" && i.IMDbID != "" {
		ids.IMDb = i.IMDbID
	}
	if ids.TVDB == 0 && i.TVDBID > 0 {
		ids.TVDB = i.TVDBID
	}
	if ids.TMDB == 0 && i.ID > 0 && (i.Mediatype == "movie" || i.Mediatype == "show") {
		// `id` on watchlist items mirrors the TMDB id for movies and shows.
		ids.TMDB = i.ID
	}
	return ids
}

type mdblistWatchlistResponse struct {
	Movies     []mdblistWatchlistItem `json:"movies"`
	Shows      []mdblistWatchlistItem `json:"shows"`
	Pagination *mdblistPagination     `json:"pagination"`
}

type mdblistUser struct {
	UserID   int    `json:"user_id"`
	Username string `json:"username"`
	Name     string `json:"name"`
}

// accountID is the built-in provider's account identity: the numeric user ID,
// or the username when MDBList omits it.
func (u mdblistUser) accountID() string {
	if u.UserID > 0 {
		return strconv.Itoa(u.UserID)
	}
	return strings.TrimSpace(u.Username)
}

// mdblistWriteResponse is the part of a sync or watchlist write response that
// decides its outcome. MDBList reports counts per kind and never names the
// items it did not accept.
type mdblistWriteResponse struct {
	NotFound json.RawMessage `json:"not_found"`
	Errors   json.RawMessage `json:"errors"`
}

type mdblistWatchedPayload struct {
	Movies   []mdblistWatchedMoviePayload   `json:"movies,omitempty"`
	Episodes []mdblistWatchedEpisodePayload `json:"episodes,omitempty"`
}

type mdblistWatchedMoviePayload struct {
	IDs       mdblistIDs `json:"ids"`
	WatchedAt string     `json:"watched_at,omitempty"`
}

type mdblistRef struct {
	IDs mdblistIDs `json:"ids"`
}

type mdblistWatchedEpisodePayload struct {
	IDs       mdblistIDs  `json:"ids,omitempty"`
	Show      *mdblistRef `json:"show,omitempty"`
	Season    int         `json:"season"`
	Episode   int         `json:"episode"`
	WatchedAt string      `json:"watched_at,omitempty"`
}

type mdblistListPayload struct {
	Movies []mdblistRef `json:"movies,omitempty"`
	Shows  []mdblistRef `json:"shows,omitempty"`
}

// emptyJSONValue reports whether a not_found or errors value holds nothing:
// absent, null, false, zero, blank, or containers of only such values.
func emptyJSONValue(raw json.RawMessage) bool {
	if len(bytes.TrimSpace(raw)) == 0 {
		return true
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return false
	}
	return emptyJSONTree(value)
}

func emptyJSONTree(value any) bool {
	switch typed := value.(type) {
	case nil:
		return true
	case bool:
		return !typed
	case float64:
		return typed == 0
	case string:
		return strings.TrimSpace(typed) == ""
	case []any:
		for _, child := range typed {
			if !emptyJSONTree(child) {
				return false
			}
		}
		return true
	case map[string]any:
		for _, child := range typed {
			if !emptyJSONTree(child) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func intFromJSON(value any) int {
	switch v := value.(type) {
	case float64:
		return int(v)
	case string:
		parsed, _ := strconv.Atoi(v)
		return parsed
	default:
		return 0
	}
}

func stringFromJSON(value any) string {
	if s, ok := value.(string); ok {
		return s
	}
	return ""
}
