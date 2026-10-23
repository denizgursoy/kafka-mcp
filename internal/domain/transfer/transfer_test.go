package transfer

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// echoArgs is a download that writes the arguments its link carried, so a
// test sees exactly what came back out of the link.
func echoArgs(_ context.Context, args json.RawMessage) (any, error) {
	return Download(func(_ context.Context, w io.Writer) (any, error) {
		_, err := w.Write(args)
		return nil, err
	}), nil
}

func hub(t *testing.T, secret string) *Hub {
	t.Helper()

	h, err := NewHub("/mcp/x/files", []byte(secret))
	if err != nil {
		t.Fatal(err)
	}
	h.Register("echo", echoArgs)

	return h
}

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

func TestALinkOpensOnEveryInstanceSharingTheKey(t *testing.T) {
	maker, other := hub(t, "shared"), hub(t, "shared")
	makerURL, otherURL := serve(t, maker), serve(t, other)

	ticket, err := maker.Offer(t.Context(), makerURL, "echo", map[string]string{"topic": "orders"}, "it's.csv", "text/csv")
	if err != nil {
		t.Fatal(err)
	}
	if want := `-o 'it'\''s.csv.part'`; !strings.Contains(ticket.Command, want) {
		t.Fatalf("the file name must be shell-quoted in the command, got %s", ticket.Command)
	}
	if strings.Contains(ticket.URL, "orders") {
		t.Fatalf("a link must not show what it runs, or scripts end up in proxy logs: %s", ticket.URL)
	}

	// The load balancer sends the fetch to the other instance.
	elsewhere := otherURL + strings.TrimPrefix(ticket.URL, makerURL)
	code, body, _ := do(t, http.MethodGet, elsewhere, "")
	if code != http.StatusOK || body != `{"topic":"orders"}` {
		t.Fatalf("an instance sharing the key must run a link another made, with its arguments intact: %d %q", code, body)
	}
}

func TestALinkDoesNotOpenWithAnotherKeyOrEndpointOrWhenChanged(t *testing.T) {
	maker := hub(t, "shared")
	ticket, err := maker.Offer(t.Context(), "http://x", "echo", "a", "f", "text/plain")
	if err != nil {
		t.Fatal(err)
	}
	token := ticket.URL[strings.LastIndex(ticket.URL, "/")+1:]

	stranger := hub(t, "different")
	if _, err := stranger.open(token); !errors.Is(err, errBadLink) {
		t.Fatalf("a link sealed with another key must not open: %v", err)
	}

	otherEndpoint, _ := NewHub("/mcp/y/files", []byte("shared"))
	if _, err := otherEndpoint.open(token); !errors.Is(err, errBadLink) {
		t.Fatalf("a link made for one endpoint must not open on another sharing the secret: %v", err)
	}

	changed := []byte(token)
	changed[len(changed)/2] ^= 1
	if _, err := maker.open(string(changed)); !errors.Is(err, errBadLink) {
		t.Fatalf("a changed link must not open, or a caller could run a search nobody offered: %v", err)
	}
}

func TestLinksExpire(t *testing.T) {
	h := hub(t, "k")
	base := serve(t, h)
	now := time.Now()
	h.now = func() time.Time { return now }

	ticket, _ := h.Offer(t.Context(), base, "echo", "a", "f", "text/plain")
	now = now.Add(TTL)
	if code, body, _ := do(t, http.MethodGet, ticket.URL, ""); code != http.StatusGone || !strings.Contains(body, "expired") {
		t.Fatalf("an expired link must be refused as expired: %d %s", code, body)
	}
}

func TestAWrongMethodIsRefusedBeforeAnyWork(t *testing.T) {
	h := hub(t, "k")
	base := serve(t, h)
	runs := 0
	h.Register("count", func(context.Context, json.RawMessage) (any, error) {
		runs++
		return Download(func(context.Context, io.Writer) (any, error) { return nil, nil }), nil
	})

	ticket, _ := h.Offer(t.Context(), base, "count", nil, "f", "text/plain")
	if code, _, _ := do(t, http.MethodPut, ticket.URL, "x"); code != http.StatusMethodNotAllowed || runs != 0 {
		t.Fatalf("a download link must refuse an upload without running anything: %d, runs %d", code, runs)
	}
}

func TestAFailureBeforeTheFirstByteIsAPlainError(t *testing.T) {
	h := hub(t, "k")
	base := serve(t, h)
	h.Register("refused", func(context.Context, json.RawMessage) (any, error) {
		return nil, errors.New("the endpoint is read-only now")
	})
	h.Register("empty-fail", func(context.Context, json.RawMessage) (any, error) {
		return Download(func(context.Context, io.Writer) (any, error) { return nil, errors.New("no such topic") }), nil
	})

	for kind, want := range map[string]string{"refused": "read-only now", "empty-fail": "no such topic"} {
		ticket, _ := h.Offer(t.Context(), base, kind, nil, "f", "text/plain")
		if code, body, _ := do(t, http.MethodGet, ticket.URL, ""); code != http.StatusUnprocessableEntity || !strings.Contains(body, want) {
			t.Fatalf("%s: a failure before any data must answer with the reason, so curl -f shows it: %d %s", kind, code, body)
		}
	}
}

func TestAFailureAfterDataBreaksTheTransfer(t *testing.T) {
	h := hub(t, "k")
	base := serve(t, h)
	h.Register("partial", func(context.Context, json.RawMessage) (any, error) {
		return Download(func(_ context.Context, w io.Writer) (any, error) {
			_, _ = io.WriteString(w, strings.Repeat("partial\n", 10000))
			return nil, errors.New("the broker went away")
		}), nil
	})

	ticket, _ := h.Offer(t.Context(), base, "partial", nil, "f", "text/plain")
	if _, _, err := do(t, http.MethodGet, ticket.URL, ""); err == nil {
		t.Fatal("a download that fails after sending data must break the connection, so curl -f fails instead of keeping a cut-short file")
	}
}

func TestAnUploadReadsTheBodyAndAnswersWithTheResult(t *testing.T) {
	h := hub(t, "k")
	base := serve(t, h)
	h.Register("up", func(_ context.Context, args json.RawMessage) (any, error) {
		return Upload(func(_ context.Context, r io.Reader) (any, error) {
			body, _ := io.ReadAll(r)
			if string(body) == "bad" {
				return nil, errors.New("line 1 is not JSON")
			}
			return map[string]any{"bytes": len(body), "args": json.RawMessage(args)}, nil
		}), nil
	})

	ticket, err := h.Accept(t.Context(), base, "up", map[string]bool{"confirm": true}, "send.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	if ticket.Command != "curl -fsS -T 'send.jsonl' '"+ticket.URL+"'" {
		t.Fatalf("the upload command must send the user's file: %s", ticket.Command)
	}
	if code, _, _ := do(t, http.MethodGet, ticket.URL, ""); code != http.StatusMethodNotAllowed {
		t.Fatalf("a GET must be refused on an upload link: %d", code)
	}
	if code, body, _ := do(t, http.MethodPut, ticket.URL, "hello"); code != http.StatusOK || !strings.Contains(body, `"bytes":5`) || !strings.Contains(body, `"confirm":true`) {
		t.Fatalf("the upload must be read with the link's arguments and its result returned: %d %s", code, body)
	}
	if code, body, _ := do(t, http.MethodPost, ticket.URL, "bad"); code != http.StatusUnprocessableEntity || !strings.Contains(body, "line 1") {
		t.Fatalf("a refused upload must answer 422 with the reason: %d %s", code, body)
	}
}

func TestALinkTooLongForProxiesIsRefusedWhenMade(t *testing.T) {
	h := hub(t, "k")
	random := make([]byte, 8<<10)
	_, _ = rand.Read(random)
	if _, err := h.Offer(t.Context(), "http://x", "echo", random, "f", "text/plain"); err == nil || !strings.Contains(err.Error(), "too large") {
		t.Fatalf("a request that cannot fit in a link must be refused at once, not as a proxy error later: %v", err)
	}
}

func TestWithoutAnAddressThereIsNoLink(t *testing.T) {
	var h *Hub
	if _, err := h.Offer(t.Context(), "http://x", "echo", nil, "f", "text/plain"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("a server with no hub must refuse with the reason: %v", err)
	}
	if _, err := hub(t, "k").Offer(t.Context(), "", "echo", nil, "f", "text/plain"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("a call that arrived without an address must refuse: %v", err)
	}
}

func TestALocalHubSavesAndReadsOnThisMachine(t *testing.T) {
	dir := t.TempDir()
	h := NewLocalHub(dir)
	h.Register("echo", echoArgs)
	h.Register("fail", func(context.Context, json.RawMessage) (any, error) {
		return Download(func(_ context.Context, w io.Writer) (any, error) {
			_, _ = io.WriteString(w, "half")
			return nil, errors.New("cut short")
		}), nil
	})
	h.Register("up", func(context.Context, json.RawMessage) (any, error) {
		return Upload(func(_ context.Context, r io.Reader) (any, error) {
			body, _ := io.ReadAll(r)
			return string(body), nil
		}), nil
	})

	ticket, err := h.Offer(t.Context(), "", "echo", "data", "out/sub.json", "")
	if err == nil || !strings.Contains(err.Error(), "out") {
		// A missing subdirectory is the caller's to create.
		t.Fatalf("saving into a directory that does not exist must fail with its name: %v", err)
	}

	ticket, err = h.Offer(t.Context(), "", "echo", "data", "saved.json", "")
	if err != nil {
		t.Fatal(err)
	}
	if ticket.Method != MethodLocal || ticket.Path != filepath.Join(dir, "saved.json") || ticket.URL != "" {
		t.Fatalf("over stdio the file must be saved right away, with no link: %+v", ticket)
	}
	if body, _ := os.ReadFile(ticket.Path); string(body) != `"data"` {
		t.Fatalf("the saved file must hold the download: %q", body)
	}

	if _, err := h.Offer(t.Context(), "", "echo", "again", "saved.json", ""); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("an existing file must never be replaced: %v", err)
	}
	if body, _ := os.ReadFile(ticket.Path); string(body) != `"data"` {
		t.Fatalf("the refused save must leave the existing file alone: %q", body)
	}

	if _, err := h.Offer(t.Context(), "", "fail", nil, "broken.json", ""); err == nil {
		t.Fatal("a failed download must be reported")
	}
	if _, err := os.Stat(filepath.Join(dir, "broken.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("a failed download must leave no file that looks complete")
	}
	if leftovers, _ := filepath.Glob(filepath.Join(dir, ".*.part")); len(leftovers) != 0 {
		t.Fatalf("a failed download must clean up its partial file: %v", leftovers)
	}

	read, err := h.Accept(t.Context(), "", "up", nil, "saved.json")
	if err != nil || read.Result != `"data"` || read.Path != filepath.Join(dir, "saved.json") {
		t.Fatalf("an upload over stdio must read the local file now: %+v %v", read, err)
	}
	if _, err := h.Accept(t.Context(), "", "up", nil, "missing.json"); err == nil {
		t.Fatal("a missing local file must be reported")
	}
}

func TestCheckFilename(t *testing.T) {
	for name, local := range map[string]bool{"a.csv": false, "dir/a.csv": true, "/tmp/a.csv": true} {
		if err := CheckFilename(name, local); err != nil {
			t.Fatalf("%q (local %v) must be accepted: %v", name, local, err)
		}
	}
	for name, local := range map[string]bool{"dir/a.csv": false, "..": false, "": true, "a\x00b": true} {
		if err := CheckFilename(name, local); err == nil {
			t.Fatalf("%q (local %v) must be refused", name, local)
		}
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
