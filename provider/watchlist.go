package provider

import (
	"context"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
)

// writeWatchlist adds or removes watchlist titles in one request. MDBList
// answers with per-kind counts and never names the titles it did not accept:
//   - an add with any not_found entry confirms no title, so the batch is
//     retried;
//   - a removal with any not_found entry still leaves every title absent,
//     which is the desired state;
//   - a title MDBList cannot identify is left out of the request and rejected.
//
// MDBList's API has no way to order the watchlist, so list_position is
// ignored.
func (c *apiClient) writeWatchlist(ctx context.Context, events []*pluginv1.WatchSyncEvent, results *resultSet, removing bool) *pluginv1.WatchSyncFault {
	var payload mdblistListPayload
	sent := make([]*pluginv1.WatchSyncEvent, 0, len(events))
	for _, event := range events {
		ids, ok := listItemIDs(event)
		if !ok {
			results.reject(event, "MDBList watchlist sync requires a movie or series with an external ID")
			continue
		}
		if event.GetMedia().GetMediaType() == pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_MOVIE {
			payload.Movies = append(payload.Movies, mdblistRef{IDs: ids})
		} else {
			payload.Shows = append(payload.Shows, mdblistRef{IDs: ids})
		}
		sent = append(sent, event)
	}
	if len(sent) == 0 {
		return nil
	}
	path := "/watchlist/items/add"
	if removing {
		path = "/watchlist/items/remove"
	}
	var response mdblistWriteResponse
	if err := c.post(ctx, path, payload, &response); err != nil {
		return failBatch(sent, results, faultFor(err))
	}
	notFound := !emptyJSONValue(response.NotFound)
	for _, event := range sent {
		switch {
		case !notFound:
			results.set(event, pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED)
		case removing:
			results.set(event, pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_NO_CHANGE)
		default:
			results.fail(event, temporaryFault("MDBList did not accept one or more watchlist items in the batch"))
		}
	}
	return nil
}
