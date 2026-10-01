// Package provider implements the MDBList watch-sync provider. Each Silo
// profile connects with its own MDBList API key, which MDBList expects as the
// apikey query parameter.
package provider

import (
	"context"
	"net/http"
	"strings"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
)

const (
	capabilityID = "mdblist"

	// ApplyEvents stops starting new work after this long, or sooner when the
	// call's deadline, less syncDeadlineMargin, comes first, so the host
	// receives the finished results before its RPC deadline.
	syncBudget         = 90 * time.Second
	syncDeadlineMargin = 5 * time.Second
)

type Server struct {
	pluginv1.UnimplementedWatchSyncProviderServer
	http     *http.Client
	baseURL  string
	limiters *keyLimiters
	now      func() time.Time
	sleep    func(context.Context, time.Duration) error
}

func NewServer(httpClient *http.Client) *Server {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: defaultRequestTimeout}
	}
	return &Server{
		http:     httpClient,
		baseURL:  defaultBaseURL,
		limiters: newKeyLimiters(requestInterval, requestBurst),
		now:      time.Now,
		sleep:    sleepContext,
	}
}

func (s *Server) ExchangeAPIKey(ctx context.Context, req *pluginv1.WatchSyncExchangeAPIKeyRequest) (*pluginv1.WatchSyncCredentialResponse, error) {
	if fault := checkCapability(req.GetCapabilityId()); fault != nil {
		return &pluginv1.WatchSyncCredentialResponse{Fault: fault}, nil
	}
	apiKey := strings.TrimSpace(req.GetApiKey())
	if apiKey == "" {
		return &pluginv1.WatchSyncCredentialResponse{Fault: invalidRequestFault("MDBList API key is required")}, nil
	}
	account, fault := s.account(ctx, s.client(apiKey))
	if fault != nil {
		return &pluginv1.WatchSyncCredentialResponse{Fault: fault}, nil
	}
	return &pluginv1.WatchSyncCredentialResponse{
		Credentials: &pluginv1.WatchSyncCredentials{AccessToken: apiKey},
		Account:     account,
	}, nil
}

// RefreshCredentials returns the stored credentials unchanged: MDBList API
// keys do not expire, and a refresh must not spend the account's quota.
func (s *Server) RefreshCredentials(_ context.Context, req *pluginv1.WatchSyncRefreshCredentialsRequest) (*pluginv1.WatchSyncCredentialResponse, error) {
	if _, fault := s.authenticatedClient(req.GetContext()); fault != nil {
		return &pluginv1.WatchSyncCredentialResponse{Fault: fault}, nil
	}
	credentials := req.GetContext().GetCredentials()
	return &pluginv1.WatchSyncCredentialResponse{
		Credentials: &pluginv1.WatchSyncCredentials{
			AccessToken:      credentials.GetAccessToken(),
			RefreshToken:     credentials.GetRefreshToken(),
			ExpiresAt:        credentials.GetExpiresAt(),
			TokenType:        credentials.GetTokenType(),
			Scopes:           append([]string(nil), credentials.GetScopes()...),
			SecretAttributes: cloneMap(credentials.GetSecretAttributes()),
		},
	}, nil
}

func (s *Server) GetAccount(ctx context.Context, req *pluginv1.WatchSyncGetAccountRequest) (*pluginv1.WatchSyncGetAccountResponse, error) {
	client, fault := s.authenticatedClient(req.GetContext())
	if fault != nil {
		return &pluginv1.WatchSyncGetAccountResponse{Fault: fault}, nil
	}
	account, fault := s.account(ctx, client)
	return &pluginv1.WatchSyncGetAccountResponse{Account: account, Fault: fault}, nil
}

func (s *Server) account(ctx context.Context, client *apiClient) (*pluginv1.WatchSyncAccount, *pluginv1.WatchSyncFault) {
	var user mdblistUser
	if err := client.get(ctx, "/user", nil, &user); err != nil {
		return nil, faultFor(err)
	}
	id := user.accountID()
	if id == "" {
		return nil, permanentFault("MDBList did not return an account for the API key")
	}
	return &pluginv1.WatchSyncAccount{
		ExternalSubject: id,
		Username:        user.Username,
		DisplayName:     firstNonEmpty(user.Name, user.Username),
	}, nil
}

func (s *Server) ListRemoteState(ctx context.Context, req *pluginv1.WatchSyncListRemoteStateRequest) (*pluginv1.WatchSyncListRemoteStateResponse, error) {
	client, fault := s.authenticatedClient(req.GetContext())
	if fault != nil {
		return &pluginv1.WatchSyncListRemoteStateResponse{Fault: fault}, nil
	}
	kind, fault := requestedStateKind(req.GetStateKinds())
	if fault != nil {
		return &pluginv1.WatchSyncListRemoteStateResponse{Fault: fault}, nil
	}
	switch kind {
	case pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_WATCHED:
		return listWatched(ctx, client, req.GetPageToken()), nil
	case pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_PROGRESS:
		return s.listProgress(ctx, client, req.GetPageToken()), nil
	case pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_WATCHLIST:
		return listWatchlist(ctx, client, req.GetPageToken()), nil
	case pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_RATING:
		return listRatings(ctx, client, req.GetPageToken()), nil
	default:
		return &pluginv1.WatchSyncListRemoteStateResponse{
			Fault: invalidRequestFault("MDBList does not support the requested state family"),
		}, nil
	}
}

// ApplyEvents applies each run of consecutive events that share an operation
// as one group, so a batch of watched plays, watchlist items, or ratings costs
// one MDBList write. Groups run in order, and a rate limit or the time box
// leaves every later event for a retry.
func (s *Server) ApplyEvents(ctx context.Context, req *pluginv1.WatchSyncApplyEventsRequest) (*pluginv1.WatchSyncApplyEventsResponse, error) {
	client, fault := s.authenticatedClient(req.GetContext())
	if fault != nil {
		return &pluginv1.WatchSyncApplyEventsResponse{Fault: fault}, nil
	}
	budget := syncTimeBox(ctx)
	ctx, cancel := context.WithTimeout(ctx, syncRequestLimit(ctx))
	defer cancel()
	run := &applyRun{server: s, client: client, startedAt: s.now(), budget: budget}

	events := req.GetEvents()
	response := &pluginv1.WatchSyncApplyEventsResponse{
		Results: make([]*pluginv1.WatchSyncApplyResult, 0, len(events)),
	}
	for start := 0; start < len(events); {
		end := start + 1
		for end < len(events) && events[end].GetOperation() == events[start].GetOperation() {
			end++
		}
		group := events[start:end]
		start = end
		if run.stop != nil {
			response.Results = append(response.Results, resultsFromFault(group, run.stop)...)
			continue
		}
		if run.outOfTime() {
			run.stop = temporaryFault("MDBList sync time limit reached; the event will be retried")
			response.Results = append(response.Results, resultsFromFault(group, run.stop)...)
			continue
		}
		results, connectionFault := run.apply(ctx, group)
		if connectionFault != nil {
			if connectionFault.GetCode() == pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_CREDENTIAL {
				return &pluginv1.WatchSyncApplyEventsResponse{Fault: connectionFault}, nil
			}
			// A rate limit keeps the results already settled and leaves this
			// group's unsettled events, and every later one, for a retry.
			run.stop = connectionFault
		}
		response.Results = append(response.Results, results...)
	}
	return response, nil
}

// applyRun is the state one ApplyEvents call shares across its groups.
type applyRun struct {
	server    *Server
	client    *apiClient
	startedAt time.Time
	budget    time.Duration
	// stop is the fault every remaining event is answered with once a rate
	// limit or the time box ends the call.
	stop *pluginv1.WatchSyncFault
}

func (r *applyRun) outOfTime() bool {
	return r.server.now().Sub(r.startedAt) >= r.budget
}

// apply applies one group of events that share an operation. It returns one
// result per event, plus a fault when a rate limit or a rejected API key
// affects the whole connection.
func (r *applyRun) apply(ctx context.Context, events []*pluginv1.WatchSyncEvent) ([]*pluginv1.WatchSyncApplyResult, *pluginv1.WatchSyncFault) {
	pending := make([]*pluginv1.WatchSyncEvent, 0, len(events))
	results := newResultSet(events)
	for _, event := range events {
		if strings.TrimSpace(event.GetEventId()) == "" {
			results.reject(event, "Watch event ID is required")
			continue
		}
		pending = append(pending, event)
	}
	var fault *pluginv1.WatchSyncFault
	switch events[0].GetOperation() {
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_MARK_WATCHED:
		fault = r.markWatched(ctx, pending, results)
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_MARK_UNWATCHED:
		fault = r.client.markUnwatched(ctx, pending, results)
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_ADD_TO_WATCHLIST:
		fault = r.client.writeWatchlist(ctx, pending, results, false)
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_REMOVE_FROM_WATCHLIST:
		fault = r.client.writeWatchlist(ctx, pending, results, true)
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SET_RATING:
		fault = r.client.writeRatings(ctx, pending, results, false)
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_REMOVE_RATING:
		fault = r.client.writeRatings(ctx, pending, results, true)
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SCROBBLE_START,
		pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SCROBBLE_PAUSE,
		pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SCROBBLE_STOP:
		fault = r.scrobble(ctx, pending, results)
	default:
		for _, event := range pending {
			results.reject(event, "MDBList does not support this watch operation")
		}
	}
	if fault != nil && fault.GetCode() == pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_RATE_LIMITED {
		results.settleRemaining(fault)
	}
	return results.list(), fault
}

// resultSet collects one result per event of a group, in event order.
type resultSet struct {
	events  []*pluginv1.WatchSyncEvent
	results map[*pluginv1.WatchSyncEvent]*pluginv1.WatchSyncApplyResult
}

func newResultSet(events []*pluginv1.WatchSyncEvent) *resultSet {
	return &resultSet{events: events, results: make(map[*pluginv1.WatchSyncEvent]*pluginv1.WatchSyncApplyResult, len(events))}
}

func (r *resultSet) set(event *pluginv1.WatchSyncEvent, status pluginv1.WatchSyncApplyStatus) {
	r.results[event] = &pluginv1.WatchSyncApplyResult{EventId: event.GetEventId(), Status: status}
}

func (r *resultSet) fail(event *pluginv1.WatchSyncEvent, fault *pluginv1.WatchSyncFault) {
	r.results[event] = resultFromFault(event.GetEventId(), fault)
}

func (r *resultSet) reject(event *pluginv1.WatchSyncEvent, message string) {
	r.fail(event, invalidRequestFault(message))
}

// settleRemaining answers every event that has no result yet with fault.
func (r *resultSet) settleRemaining(fault *pluginv1.WatchSyncFault) {
	for _, event := range r.events {
		if _, ok := r.results[event]; !ok {
			r.fail(event, fault)
		}
	}
}

func (r *resultSet) list() []*pluginv1.WatchSyncApplyResult {
	r.settleRemaining(temporaryFault("MDBList did not confirm the event; it will be retried"))
	out := make([]*pluginv1.WatchSyncApplyResult, 0, len(r.events))
	for _, event := range r.events {
		out = append(out, r.results[event])
	}
	return out
}

// syncTimeBox returns how long a call keeps starting new work: syncBudget,
// cut short so the results still reach the host before the call's deadline.
func syncTimeBox(ctx context.Context) time.Duration {
	budget := syncBudget
	if deadline, ok := ctx.Deadline(); ok {
		budget = min(budget, time.Until(deadline)-syncDeadlineMargin)
	}
	return budget
}

// syncRequestLimit bounds every request of one call to end syncDeadlineMargin
// before the call's deadline.
func syncRequestLimit(ctx context.Context) time.Duration {
	limit := syncBudget + defaultRequestTimeout
	if deadline, ok := ctx.Deadline(); ok {
		limit = min(limit, max(time.Until(deadline)-syncDeadlineMargin, 0))
	}
	return limit
}

func (s *Server) authenticatedClient(auth *pluginv1.WatchSyncAuthenticatedContext) (*apiClient, *pluginv1.WatchSyncFault) {
	if fault := checkCapability(auth.GetCapabilityId()); fault != nil {
		return nil, fault
	}
	apiKey := strings.TrimSpace(auth.GetCredentials().GetAccessToken())
	if apiKey == "" {
		return nil, invalidCredentialFault("MDBList API key is missing; reconnect MDBList")
	}
	return s.client(apiKey), nil
}

func (s *Server) client(apiKey string) *apiClient {
	return &apiClient{
		baseURL: strings.TrimRight(s.baseURL, "/"),
		apiKey:  apiKey,
		http:    s.http,
		limiter: s.limiters.forKey(apiKey),
		sleep:   s.sleep,
	}
}

func checkCapability(requested string) *pluginv1.WatchSyncFault {
	if requested != capabilityID {
		return invalidRequestFault("Unknown MDBList capability")
	}
	return nil
}

func requestedStateKind(kinds []pluginv1.WatchSyncRemoteStateKind) (pluginv1.WatchSyncRemoteStateKind, *pluginv1.WatchSyncFault) {
	if len(kinds) == 0 {
		return pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_WATCHED, nil
	}
	if len(kinds) != 1 {
		return 0, invalidRequestFault("MDBList accepts one state family per traversal")
	}
	return kinds[0], nil
}

func resultFromFault(eventID string, fault *pluginv1.WatchSyncFault) *pluginv1.WatchSyncApplyResult {
	status := pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_REJECTED
	if fault.GetCode() == pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_TEMPORARY ||
		fault.GetCode() == pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_RATE_LIMITED {
		status = pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_RETRY
	}
	return &pluginv1.WatchSyncApplyResult{EventId: eventID, Status: status, Fault: fault}
}

func resultsFromFault(events []*pluginv1.WatchSyncEvent, fault *pluginv1.WatchSyncFault) []*pluginv1.WatchSyncApplyResult {
	results := make([]*pluginv1.WatchSyncApplyResult, 0, len(events))
	for _, event := range events {
		results = append(results, resultFromFault(event.GetEventId(), fault))
	}
	return results
}

// failBatch answers every event of one write with fault, or returns a
// connection-wide fault for the caller to end the call with. MDBList rejects
// a write as a whole and does not say which entry it refused, so a rejected
// write of several events is retried rather than rejecting events MDBList may
// have accepted on their own.
func failBatch(events []*pluginv1.WatchSyncEvent, results *resultSet, fault *pluginv1.WatchSyncFault) *pluginv1.WatchSyncFault {
	if connectionWide(fault) {
		return fault
	}
	if len(events) > 1 && resultFromFault("", fault).GetStatus() == pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_REJECTED {
		fault = temporaryFault("MDBList rejected a batch that held this event; it will be retried")
	}
	for _, event := range events {
		results.fail(event, fault)
	}
	return nil
}

// connectionWide reports whether a fault affects every request of the
// connection, so it ends the call instead of failing one event.
func connectionWide(fault *pluginv1.WatchSyncFault) bool {
	return fault.GetCode() == pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_CREDENTIAL ||
		fault.GetCode() == pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_RATE_LIMITED
}

func invalidRequestFault(message string) *pluginv1.WatchSyncFault {
	return &pluginv1.WatchSyncFault{Code: pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_REQUEST, SafeMessage: message}
}

func invalidCredentialFault(message string) *pluginv1.WatchSyncFault {
	return &pluginv1.WatchSyncFault{Code: pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_CREDENTIAL, SafeMessage: message}
}

func temporaryFault(message string) *pluginv1.WatchSyncFault {
	return &pluginv1.WatchSyncFault{Code: pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_TEMPORARY, SafeMessage: message}
}

func permanentFault(message string) *pluginv1.WatchSyncFault {
	return &pluginv1.WatchSyncFault{Code: pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMANENT, SafeMessage: message}
}

func cloneMap(input map[string]string) map[string]string {
	if len(input) == 0 {
		return nil
	}
	out := make(map[string]string, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}
