package main

import (
	_ "embed"

	"github.com/Silo-Server/silo-plugin-sdk/pkg/pluginsdk/runtime"
	"github.com/Silo-Server/silo-plugin-watchprovider-mdblist/provider"
)

// version is set at build time with -ldflags "-X main.version=...".
var version string

//go:embed manifest.json
var manifestJSON []byte

func main() {
	runtime.ServeManifest(manifestJSON, version, runtime.CapabilityServers{
		WatchSyncProvider: provider.NewServer(nil),
	})
}
