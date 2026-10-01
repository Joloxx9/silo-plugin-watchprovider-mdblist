package provider

import (
	"context"
	"net/http"
	"testing"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
)

func TestExchangeAPIKeyValidatesKeyAgainstUserEndpoint(t *testing.T) {
	var gotPath, gotKey string
	s := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotKey = r.URL.Query().Get("apikey")
		writeJSON(w, `{"user_id":42,"username":"kingsly","name":"Kingsly Test"}`)
	})
	response, err := s.ExchangeAPIKey(context.Background(), &pluginv1.WatchSyncExchangeAPIKeyRequest{
		CapabilityId: capabilityID,
		ApiKey:       "  test-key  ",
	})
	if err != nil || response.GetFault() != nil {
		t.Fatalf("exchange: %v %v", err, response.GetFault())
	}
	if gotPath != "/user" || gotKey != "test-key" {
		t.Fatalf("request = %s apikey %q, want /user with the trimmed key", gotPath, gotKey)
	}
	credentials := response.GetCredentials()
	// Migrated connections carry only the access token, so the plugin must
	// not depend on any other credential field.
	if credentials.GetAccessToken() != "test-key" || credentials.GetRefreshToken() != "" ||
		credentials.GetTokenType() != "" || len(credentials.GetSecretAttributes()) != 0 || credentials.GetExpiresAt() != nil {
		t.Fatalf("credentials = %v, want the key as the access token only", credentials)
	}
	account := response.GetAccount()
	if account.GetExternalSubject() != "42" || account.GetUsername() != "kingsly" || account.GetDisplayName() != "Kingsly Test" {
		t.Fatalf("account = %v", account)
	}
}

func TestExchangeAPIKeyFallsBackToUsernameIdentity(t *testing.T) {
	s := newTestServer(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, `{"username":"kingsly"}`)
	})
	response, _ := s.ExchangeAPIKey(context.Background(), &pluginv1.WatchSyncExchangeAPIKeyRequest{CapabilityId: capabilityID, ApiKey: "k"})
	if response.GetAccount().GetExternalSubject() != "kingsly" {
		t.Fatalf("account = %v, want the username as identity", response.GetAccount())
	}
}

func TestExchangeAPIKeyRejectsMissingIdentity(t *testing.T) {
	s := newTestServer(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, `{}`)
	})
	response, _ := s.ExchangeAPIKey(context.Background(), &pluginv1.WatchSyncExchangeAPIKeyRequest{CapabilityId: capabilityID, ApiKey: "k"})
	if response.GetFault().GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMANENT || response.GetCredentials() != nil {
		t.Fatalf("response = %v, want a permanent fault and no credentials", response)
	}
}

func TestExchangeAPIKeyRejectsEmptyKeyWithoutCallingMDBList(t *testing.T) {
	s := newTestServer(t, func(http.ResponseWriter, *http.Request) {
		t.Error("MDBList should not be called for an empty key")
	})
	response, _ := s.ExchangeAPIKey(context.Background(), &pluginv1.WatchSyncExchangeAPIKeyRequest{CapabilityId: capabilityID, ApiKey: "   "})
	if response.GetFault().GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_REQUEST {
		t.Fatalf("fault = %v, want INVALID_REQUEST", response.GetFault())
	}
}

func TestExchangeAPIKeyReportsRejectedKeyAsInvalidCredential(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		s := newTestServer(t, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(status)
		})
		response, _ := s.ExchangeAPIKey(context.Background(), &pluginv1.WatchSyncExchangeAPIKeyRequest{CapabilityId: capabilityID, ApiKey: "wrong-key"})
		if response.GetFault().GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_CREDENTIAL {
			t.Fatalf("status %d: fault = %v, want INVALID_CREDENTIAL", status, response.GetFault())
		}
	}
}

func TestRefreshCredentialsReturnsStoredKeyWithoutCallingMDBList(t *testing.T) {
	s := newTestServer(t, func(http.ResponseWriter, *http.Request) {
		t.Error("a refresh should not spend MDBList quota")
	})
	response, err := s.RefreshCredentials(context.Background(), &pluginv1.WatchSyncRefreshCredentialsRequest{Context: authContext(testKey)})
	if err != nil || response.GetFault() != nil {
		t.Fatalf("refresh: %v %v", err, response.GetFault())
	}
	if response.GetCredentials().GetAccessToken() != testKey {
		t.Fatalf("credentials = %v, want the stored key", response.GetCredentials())
	}
}

func TestGetAccountReadsUserEndpoint(t *testing.T) {
	s := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("apikey") != testKey {
			t.Errorf("apikey = %q", r.URL.Query().Get("apikey"))
		}
		writeJSON(w, `{"user_id":7,"username":"someone"}`)
	})
	response, _ := s.GetAccount(context.Background(), &pluginv1.WatchSyncGetAccountRequest{Context: authContext(testKey)})
	if response.GetFault() != nil || response.GetAccount().GetExternalSubject() != "7" {
		t.Fatalf("response = %v", response)
	}
}

func TestAuthenticatedCallsRequireTheCapabilityAndAKey(t *testing.T) {
	s := newTestServer(t, func(http.ResponseWriter, *http.Request) {
		t.Error("MDBList should not be called")
	})
	wrongCapability := authContext(testKey)
	wrongCapability.CapabilityId = "trakt"
	response, _ := s.GetAccount(context.Background(), &pluginv1.WatchSyncGetAccountRequest{Context: wrongCapability})
	if response.GetFault().GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_REQUEST {
		t.Fatalf("wrong capability fault = %v", response.GetFault())
	}
	response, _ = s.GetAccount(context.Background(), &pluginv1.WatchSyncGetAccountRequest{Context: authContext("")})
	if response.GetFault().GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_CREDENTIAL {
		t.Fatalf("missing key fault = %v", response.GetFault())
	}
}

func TestListRemoteStateRejectsUnsupportedFamilies(t *testing.T) {
	s := newTestServer(t, func(http.ResponseWriter, *http.Request) {
		t.Error("MDBList should not be called")
	})
	cases := map[string][]pluginv1.WatchSyncRemoteStateKind{
		"favorites": {pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_FAVORITE},
		"two kinds": {
			pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_WATCHED,
			pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_RATING,
		},
	}
	for name, kinds := range cases {
		response, _ := s.ListRemoteState(context.Background(), &pluginv1.WatchSyncListRemoteStateRequest{
			Context: authContext(testKey), StateKinds: kinds,
		})
		if response.GetFault().GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_REQUEST {
			t.Fatalf("%s: fault = %v, want INVALID_REQUEST", name, response.GetFault())
		}
	}
}

func TestListRemoteStateRejectsAForeignPageToken(t *testing.T) {
	s := newTestServer(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, `{"movies":[],"shows":[],"pagination":{"next_cursor":"c2"}}`)
	})
	first, _ := s.ListRemoteState(context.Background(), &pluginv1.WatchSyncListRemoteStateRequest{
		Context:    authContext(testKey),
		StateKinds: []pluginv1.WatchSyncRemoteStateKind{pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_WATCHLIST},
	})
	if first.GetNextPageToken() == "" {
		t.Fatalf("first page = %v, want a page token", first)
	}
	for _, token := range []string{first.GetNextPageToken(), "not base64!"} {
		response, _ := s.ListRemoteState(context.Background(), &pluginv1.WatchSyncListRemoteStateRequest{
			Context:    authContext(testKey),
			PageToken:  token,
			StateKinds: []pluginv1.WatchSyncRemoteStateKind{pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_RATING},
		})
		if response.GetFault().GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_REQUEST {
			t.Fatalf("token %q: fault = %v, want INVALID_REQUEST", token, response.GetFault())
		}
	}
}

func TestApplyEventsRejectsUnsupportedOperationsWithoutCallingMDBList(t *testing.T) {
	s := newTestServer(t, func(http.ResponseWriter, *http.Request) {
		t.Error("MDBList should not be called")
	})
	response := applyEvents(t, s,
		movieEvent("fav", pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_ADD_FAVORITE, map[string]string{"imdb": "tt0111161"}),
		movieEvent("", pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_MARK_WATCHED, map[string]string{"imdb": "tt0111161"}),
	)
	result := assertStatus(t, response, "fav", pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_REJECTED)
	if result.GetFault().GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_REQUEST {
		t.Fatalf("fault = %v", result.GetFault())
	}
	assertStatus(t, response, "", pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_REJECTED)
}

func TestApplyEventsReportsRejectedKeyForTheWholeRequest(t *testing.T) {
	s := newTestServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	})
	response := applyEvents(t, s,
		movieEvent("w1", pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_ADD_TO_WATCHLIST, map[string]string{"imdb": "tt0111161"}),
	)
	if response.GetFault().GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_CREDENTIAL || len(response.GetResults()) != 0 {
		t.Fatalf("response = %v, want one connection-wide INVALID_CREDENTIAL fault", response)
	}
}

func TestApplyEventsLeavesLaterGroupsForRetryAfterARateLimit(t *testing.T) {
	var paths []string
	s := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		if r.URL.Path == "/watchlist/items/add" {
			writeJSON(w, `{"added":{"movies":1}}`)
			return
		}
		w.Header().Set("Retry-After", "3600")
		w.WriteHeader(http.StatusTooManyRequests)
	})
	rating := movieEvent("rate", pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SET_RATING, map[string]string{"imdb": "tt0111161"})
	rating.Rating = 8
	response := applyEvents(t, s,
		movieEvent("add", pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_ADD_TO_WATCHLIST, map[string]string{"imdb": "tt0111161"}),
		rating,
		movieEvent("remove", pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_REMOVE_FROM_WATCHLIST, map[string]string{"imdb": "tt0111161"}),
	)
	assertStatus(t, response, "add", pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED)
	for _, id := range []string{"rate", "remove"} {
		result := assertStatus(t, response, id, pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_RETRY)
		if result.GetFault().GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_RATE_LIMITED ||
			result.GetFault().GetRetryAfter().AsDuration().Hours() != 1 {
			t.Fatalf("%s fault = %v, want RATE_LIMITED for an hour", id, result.GetFault())
		}
	}
	if len(paths) != 2 {
		t.Fatalf("requests = %v, want the rate-limited write to stop the batch", paths)
	}
}

func TestApplyEventsRetriesEverythingWhenTheDeadlineLeavesNoTime(t *testing.T) {
	s := newTestServer(t, func(http.ResponseWriter, *http.Request) {
		t.Error("MDBList should not be called without time to answer the host")
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	response, _ := s.ApplyEvents(ctx, &pluginv1.WatchSyncApplyEventsRequest{
		Context: authContext(testKey),
		Events: []*pluginv1.WatchSyncEvent{
			movieEvent("w1", pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_ADD_TO_WATCHLIST, map[string]string{"imdb": "tt1"}),
			movieEvent("s1", pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SCROBBLE_START, map[string]string{"imdb": "tt1"}),
		},
	})
	for _, id := range []string{"w1", "s1"} {
		result := assertStatus(t, response, id, statusRetry)
		if result.GetFault().GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_TEMPORARY {
			t.Fatalf("%s fault = %v", id, result.GetFault())
		}
	}
}

func TestMarkWatchedStillWritesWhenTheHistoryReadRunsOutOfTime(t *testing.T) {
	fake := &historyServer{history: `{"movies":[]}`, writeResponse: `{}`}
	s := newTestServer(t, fake.handle(t))
	start := time.Now()
	calls := 0
	s.now = func() time.Time {
		calls++
		if calls == 1 {
			return start
		}
		// Every later check sees most of the time box spent.
		return start.Add(60 * time.Second)
	}
	response := applyEvents(t, s, at(movieEvent("h1", markWatched, map[string]string{"imdb": "tt0111161"}), time.Now()))
	assertStatus(t, response, "h1", statusApplied)
	if len(fake.reads) != 0 || len(fake.writes) != 1 {
		t.Fatalf("reads = %v writes = %v, want the write without the history read", fake.reads, fake.writes)
	}
}
