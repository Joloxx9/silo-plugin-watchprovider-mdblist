# MDBList watch-provider plugin for Silo

Syncs Silo profiles with an [MDBList](https://mdblist.com) account through Silo's `watch_sync_provider.v1` plugin contract. It replaces the MDBList provider built into earlier Silo releases; the differences are listed under [Upgrading from the built-in provider](#upgrading-from-the-built-in-provider).

## Capabilities

- Imports movie and episode watch history, one entry per play.
- Exports completed movie and episode watches, and marks titles unwatched.
- Imports resume progress from paused MDBList playback sessions.
- Imports, exports, and removes watchlist movies and series, and keeps MDBList's watchlist order.
- Imports and exports movie and series ratings, including removals.
- Sends live playback start, pause, and stop events.

MDBList has one personal list, its watchlist, so the plugin syncs it with Silo's watchlist and does not advertise favorites.

## Setup

1. Install the plugin. It has no server-wide settings.
2. In Silo's watch-provider settings, connect each profile with that person's MDBList API key, from the API section of [MDBList preferences](https://mdblist.com/preferences/#api).

Silo stores the key encrypted with the profile's connection. The plugin sends it to MDBList as the `apikey` query parameter and keeps it out of every error message it returns.

## How sync works

- **Watch history import** reads the whole MDBList play history on every sync. MDBList's show and season rows only summarize episode state, so the plugin imports movie and episode plays and lets Silo work out season and series completion.
- **Watch history export** first reads MDBList's plays from a day before the earliest play in the batch. A play MDBList already holds at the same second is reported as unchanged and not written again. MDBList also folds a written play into a nearby existing one, so a retried export does not create a duplicate play. The read stops after two pages (2,000 plays) so a backlog of old plays does not spend the daily quota; when it stops early or fails, the plugin writes the plays and relies on that folding.
- **Watchlist import** returns the full watchlist in MDBList's order on every sync, so Silo can mirror the order and treat missing titles as removed.
- **Ratings import** reads every movie and show rating. Silo treats a rating missing from a complete read as removed, so the plugin claims a complete read only when it can trust one:
  - A read that MDBList pages by offset, or one without a shows list, is imported without removals, because offsets shift when ratings change during the read.
  - When a cursor-paged read repeats an entry or ends short of the total MDBList reported, ratings changed during the read. The plugin abandons that read, and the next sync reads every rating again.
  - A rated title with only MDBList's own ID could be any local title. Silo skips it and handles no removals of that kind in that sync.
- **Writes** go to MDBList in batches. MDBList reports rejected entries only as counts, so when any entry of a batch is not found, the whole batch is retried. A removal that finds nothing to remove counts as done.

### Rate limits

MDBList allows 1,000 requests a day on its free tier, plus a five-minute window limit. The plugin paces each API key to about one request a second. Each page of a read holds up to 1,000 MDBList entries, so a large history costs few requests. When MDBList answers with a rate limit, the plugin retries in place if MDBList asks for 15 seconds or less, at most twice. Otherwise it tells Silo to pause the connection for the time MDBList gave, or for an hour when MDBList gave none or kept limiting.

## Upgrading from the built-in provider

Connections carry over once you run a Silo release that maps this plugin to the built-in `mdblist` provider. Existing connections keep their API key, settings, watchlist and rating records, and export history, and need no new setup. The plugin reads MDBList in full on each sync, as the built-in provider did.

Some behavior differs slightly:

- Error messages no longer quote MDBList's response text, such as its validation details.
- A ratings read that MDBList changed during the read is retried on the next sync instead of being imported without removals. A ratings read without a shows list imports movie ratings without removals instead of treating them as complete.
- A play to export that has no IMDb, TMDB, or TVDB ID is reported as not found instead of failing five times first.

## Not yet supported

These need a newer plugin contract (silo-plugin-sdk v0.21):

- Sync warnings. The plugin cannot yet tell Silo why it imported ratings without removals, so the sync run shows no warning.
- Dropped shows. MDBList keeps a dropped-shows list, which neither the built-in provider nor this plugin syncs yet.

## Development

The plugin builds against `silo-plugin-sdk` v0.20.0.

```bash
make test
make build
./plugin manifest
```

`make build-all` produces static binaries for the platforms declared in `manifest.json`.

## Contributing

Read [CONTRIBUTING.md](CONTRIBUTING.md) before opening a pull request. Changes to authentication, reconciliation, idempotency, or the watch-sync contract should start as an issue.

## License

AGPL-3.0-only.
