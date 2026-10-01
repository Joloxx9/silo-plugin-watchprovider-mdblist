package provider

import (
	"context"
	"math"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
)

// scrobble sends each playback event on its own, as the built-in provider
// did. Start and pause replace the title's session on MDBList. A stop at 80%
// or more marks the title watched on MDBList. Silo also exports that play
// through MARK_WATCHED, and MDBList folds a play written close to an existing
// one into it.
func (r *applyRun) scrobble(ctx context.Context, events []*pluginv1.WatchSyncEvent, results *resultSet) *pluginv1.WatchSyncFault {
	for _, event := range events {
		if r.outOfTime() {
			results.fail(event, temporaryFault("MDBList sync time limit reached; the event will be retried"))
			continue
		}
		payload, ok := scrobblePayload(event)
		if !ok {
			results.reject(event, "MDBList scrobbles require a movie or episode with an external ID")
			continue
		}
		if err := r.client.post(ctx, scrobblePath(event.GetOperation()), payload, nil); err != nil {
			fault := faultFor(err)
			if connectionWide(fault) {
				return fault
			}
			results.fail(event, fault)
			continue
		}
		results.set(event, pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED)
	}
	return nil
}

func scrobblePath(operation pluginv1.WatchSyncOperation) string {
	switch operation {
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SCROBBLE_PAUSE:
		return "/scrobble/pause"
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SCROBBLE_STOP:
		return "/scrobble/stop"
	default:
		return "/scrobble/start"
	}
}

// scrobblePayload names a movie by its IDs, and an episode by its series' IDs
// (its own when Silo has none) with season and episode numbers nested under
// the show, the shape MDBList's scrobble endpoints read.
func scrobblePayload(event *pluginv1.WatchSyncEvent) (map[string]any, bool) {
	progress := 0.0
	if event.GetDurationSeconds() > 0 {
		progress = event.GetPositionSeconds() / event.GetDurationSeconds() * 100
	}
	progress = min(max(progress, 0), 100)
	// MDBList validates progress as a decimal with at most five total digits.
	// Two decimal places keeps every value in the 0-100 range within that
	// contract and avoids raw floating-point expansion on the wire.
	progress = math.Round(progress*100) / 100
	payload := map[string]any{"progress": progress}
	media := event.GetMedia()
	switch media.GetMediaType() {
	case pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_MOVIE:
		ids := idsFromMedia(media.GetExternalIds())
		if ids == (mdblistIDs{}) {
			return nil, false
		}
		payload["movie"] = map[string]any{"ids": ids}
	case pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE:
		showIDs := idsFromMedia(media.GetSeriesExternalIds())
		if showIDs == (mdblistIDs{}) {
			showIDs = idsFromMedia(media.GetExternalIds())
		}
		if showIDs == (mdblistIDs{}) {
			return nil, false
		}
		payload["show"] = map[string]any{
			"ids": showIDs,
			"season": map[string]any{
				"number":  media.GetSeasonNumber(),
				"episode": map[string]any{"number": media.GetEpisodeNumber()},
			},
		}
	default:
		return nil, false
	}
	return payload, true
}
