package main

import (
	"testing"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	"github.com/Silo-Server/silo-plugin-sdk/pkg/pluginsdk/manifest"
)

func TestManifestDeclaresMDBListCapability(t *testing.T) {
	t.Parallel()
	parsed, err := manifest.Load(manifestJSON)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	if parsed.GetPluginId() != "silo.watchprovider.mdblist" {
		t.Fatalf("plugin_id = %q", parsed.GetPluginId())
	}
	capabilities := parsed.GetCapabilities()
	if len(capabilities) != 1 {
		t.Fatalf("capabilities = %d, want 1", len(capabilities))
	}
	capability := capabilities[0]
	// Silo registers this plugin under the former built-in provider's key, so
	// the capability ID and display name must match the built-in.
	if capability.GetId() != "mdblist" || capability.GetDisplayName() != "MDBList" {
		t.Fatalf("capability = %q %q, want mdblist MDBList", capability.GetId(), capability.GetDisplayName())
	}
	if len(capability.GetConfigSchema()) != 0 || len(parsed.GetGlobalConfigSchema()) != 0 {
		t.Fatalf("config schemas = %v %v, want none", capability.GetConfigSchema(), parsed.GetGlobalConfigSchema())
	}
}

func TestManifestAdvertisesBuiltInCapabilities(t *testing.T) {
	t.Parallel()
	parsed, err := manifest.Load(manifestJSON)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	descriptor := parsed.GetCapabilities()[0].GetWatchSyncProvider()
	if methods := descriptor.GetAuthMethods(); len(methods) != 1 || methods[0] != pluginv1.WatchSyncAuthMethod_WATCH_SYNC_AUTH_METHOD_API_KEY {
		t.Fatalf("auth methods = %v, want API key only", methods)
	}
	want := map[string]bool{
		"import_watched":           descriptor.GetImportWatched(),
		"import_progress":          descriptor.GetImportProgress(),
		"export_watched":           descriptor.GetExportWatched(),
		"export_unwatched":         descriptor.GetExportUnwatched(),
		"import_watchlist":         descriptor.GetImportWatchlist(),
		"export_watchlist":         descriptor.GetExportWatchlist(),
		"remove_watchlist":         descriptor.GetRemoveWatchlist(),
		"provides_watchlist_order": descriptor.GetProvidesWatchlistOrder(),
		"scrobble_playback":        descriptor.GetScrobblePlayback(),
		"import_ratings":           descriptor.GetImportRatings(),
		"export_ratings":           descriptor.GetExportRatings(),
	}
	for flag, set := range want {
		if !set {
			t.Errorf("%s is not advertised", flag)
		}
	}
	// MDBList exposes one list, its watchlist, so it binds to Silo's
	// watchlist and never to favorites.
	if descriptor.GetImportFavorites() || descriptor.GetExportFavorites() || descriptor.GetRemoveFavorites() {
		t.Fatalf("descriptor advertises favorites: %v", descriptor)
	}
	media := map[pluginv1.WatchSyncMediaType]bool{}
	for _, mediaType := range descriptor.GetSupportedMediaTypes() {
		media[mediaType] = true
	}
	if len(media) != 3 || !media[pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_MOVIE] ||
		!media[pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE] || !media[pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_SERIES] {
		t.Fatalf("supported media types = %v", descriptor.GetSupportedMediaTypes())
	}
	if descriptor.GetMaxBatchSize() != 100 {
		t.Fatalf("max_batch_size = %d, want 100", descriptor.GetMaxBatchSize())
	}
}
