package transfer

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func serve(t *testing.T, h *Hub) string {
	t.Helper()

	mux := http.NewServeMux()
	mux.Handle(h.Path()+"/", h.Handler())
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	return server.URL
}

func do(t *testing.T, method, url, body string) (int, string, error) {
	t.Helper()

	request, err := http.NewRequestWithContext(t.Context(), method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return 0, "", err
	}
	defer response.Body.Close()
	reply, err := io.ReadAll(response.Body)

	return response.StatusCode, string(reply), err
}

func TestADownloadRunsWhenFetchedAndOnlyOnce(t *testing.T) {
	h := NewHub("/mcp/x/files")
	base := serve(t, h)
	runs := 0

	ticket, err := h.Offer(base, "it's.csv", "text/csv", func(_ context.Context, w io.Writer) (any, error) {
		runs++
		_, err := io.WriteString(w, "a,b\n")
		return map[string]int{"rows": 1}, err
	})
	if err != nil {
		t.Fatal(err)
	}
	if runs != 0 {
		t.Fatal("the work must not run before the link is fetched: nothing may be prepared or stored ahead")
	}
	if !strings.HasPrefix(ticket.URL, base+"/mcp/x/files/") || len(strings.TrimPrefix(ticket.URL, base+"/mcp/x/files/")) != 43 {
		t.Fatalf("the link must be the base, the hub's path and a 256-bit token: %s", ticket.URL)
	}
	if want := `-o 'it'\''s.csv.part'`; !strings.Contains(ticket.Command, want) {
		t.Fatalf("the file name must be shell-quoted in the command, got %s", ticket.Command)
	}

	if code, body, _ := do(t, http.MethodPut, ticket.URL, "x"); code != http.StatusMethodNotAllowed || runs != 0 {
		t.Fatalf("a wrong method must be refused without spending the link: %d %s", code, body)
	}
	if code, body, _ := do(t, http.MethodGet, ticket.URL, ""); code != http.StatusOK || body != "a,b\n" || runs != 1 {
		t.Fatalf("the first fetch must run the work and return it: %d %q", code, body)
	}
	if code, _, _ := do(t, http.MethodGet, ticket.URL, ""); code != http.StatusNotFound || runs != 1 {
		t.Fatalf("a second fetch must be refused, because the link is a one-time credential: %d", code)
	}
	if code, body, _ := do(t, http.MethodGet, ticket.StatusURL, ""); code != http.StatusOK || !strings.Contains(body, `"state":"done"`) || !strings.Contains(body, `"rows":1`) {
		t.Fatalf("the status link must report the outcome: %d %s", code, body)
	}
}

func TestAFailedDownloadBreaksTheTransfer(t *testing.T) {
	h := NewHub("/f")
	base := serve(t, h)
	ticket, err := h.Offer(base, "x.jsonl", "application/x-ndjson", func(_ context.Context, w io.Writer) (any, error) {
		_, _ = io.WriteString(w, strings.Repeat("partial\n", 10000))
		return nil, errors.New("the broker went away")
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, _, err := do(t, http.MethodGet, ticket.URL, ""); err == nil {
		t.Fatal("a download that fails after sending data must break the connection, so curl -f fails instead of keeping a cut-short file")
	}
	if _, body, _ := do(t, http.MethodGet, ticket.StatusURL, ""); !strings.Contains(body, "the broker went away") {
		t.Fatalf("the status link must say why the download failed: %s", body)
	}
}

func TestAnUploadReadsTheBodyAndAnswersWithTheResult(t *testing.T) {
	h := NewHub("/f")
	base := serve(t, h)
	ticket, err := h.Accept(base, "send.jsonl", func(_ context.Context, r io.Reader) (any, error) {
		body, _ := io.ReadAll(r)
		if string(body) == "bad" {
			return nil, errors.New("line 1 is not JSON")
		}
		return map[string]int{"bytes": len(body)}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if ticket.Command != "curl -fsS -T 'send.jsonl' '"+ticket.URL+"'" {
		t.Fatalf("the upload command must send the user's file: %s", ticket.Command)
	}
	if code, _, _ := do(t, http.MethodGet, ticket.URL, ""); code != http.StatusMethodNotAllowed {
		t.Fatalf("a GET must not spend an upload link: %d", code)
	}
	if code, body, _ := do(t, http.MethodPut, ticket.URL, "hello"); code != http.StatusOK || !strings.Contains(body, `"bytes":5`) {
		t.Fatalf("the upload must be read and its result returned: %d %s", code, body)
	}

	failing, _ := h.Accept(base, "send.jsonl", func(_ context.Context, r io.Reader) (any, error) {
		_, _ = io.ReadAll(r)
		return nil, errors.New("line 1 is not JSON")
	})
	if code, body, _ := do(t, http.MethodPost, failing.URL, "bad"); code != http.StatusUnprocessableEntity || !strings.Contains(body, "line 1") {
		t.Fatalf("a refused upload must answer 422 with the reason, so curl -f fails and shows it: %d %s", code, body)
	}
}

func TestLinksExpireAndAreBounded(t *testing.T) {
	h := NewHub("/f")
	base := serve(t, h)
	now := time.Now()
	h.now = func() time.Time { return now }
	noop := func(context.Context, io.Writer) (any, error) { return nil, nil }

	ticket, _ := h.Offer(base, "x", "text/plain", noop)
	now = now.Add(TTL)
	if code, _, _ := do(t, http.MethodGet, ticket.URL, ""); code != http.StatusNotFound {
		t.Fatalf("an expired link must be refused: %d", code)
	}

	for range maxPending {
		if _, err := h.Offer(base, "x", "text/plain", noop); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := h.Offer(base, "x", "text/plain", noop); err == nil {
		t.Fatal("pending links must be bounded, so unused ones cannot grow memory without end")
	}
	now = now.Add(TTL)
	if _, err := h.Offer(base, "x", "text/plain", noop); err != nil {
		t.Fatalf("expired links must free their slots: %v", err)
	}
}

func TestWithoutAnAddressThereIsNoLink(t *testing.T) {
	var h *Hub
	if _, err := h.Offer("http://x", "f", "text/plain", nil); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("a server with no hub, as over stdio, must refuse with the reason: %v", err)
	}
	if _, err := NewHub("/f").Offer("", "f", "text/plain", nil); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("a call that arrived without an address must refuse: %v", err)
	}
}

func TestMiddlewareStampsTheAddressTheCallerReached(t *testing.T) {
	seen := ""
	handler := Middleware("")(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { seen = r.Header.Get(BaseHeader) }))

	request := httptest.NewRequest(http.MethodPost, "http://console.local:8080/mcp/x", nil)
	request.Header.Set(BaseHeader, "http://evil.example")
	handler.ServeHTTP(httptest.NewRecorder(), request)
	if seen != "http://console.local:8080" {
		t.Fatalf("the base must be the address the request reached, never a client-sent value: %q", seen)
	}

	request = httptest.NewRequest(http.MethodPost, "http://10.0.0.5/mcp/x", nil)
	request.TLS = &tls.ConnectionState{}
	request.Header.Set("X-Forwarded-Host", "kafka.example.com, inner")
	handler.ServeHTTP(httptest.NewRecorder(), request)
	if seen != "https://kafka.example.com" {
		t.Fatalf("TLS and a proxy's forwarded host must be honoured: %q", seen)
	}

	Middleware("https://public.example/")(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { seen = r.Header.Get(BaseHeader) })).
		ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "http://10.0.0.5/mcp/x", nil))
	if seen != "https://public.example" {
		t.Fatalf("a configured public URL must win: %q", seen)
	}
}
