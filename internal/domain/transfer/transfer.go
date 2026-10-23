// Package transfer moves files between an MCP caller's own machine and Kafka
// without the server ever holding them on disk.
//
// It lives in internal/domain because several tools share it: search_messages
// and transform_messages hand out downloads, and produce_message an upload.
//
// A tool cannot write to the caller's machine: an MCP result goes to the
// model, not to a file. So a tool registers the work and returns a link, and
// the caller's agent fetches it there, e.g. with curl. The work runs when the
// link is used, streaming from Kafka into the response or from the request
// into Kafka, so the server keeps nothing but the pending entry in memory.
//
// A link works once and expires. Its token is 256 random bits, so it is a
// bearer credential for that one transfer, like a presigned URL: an agent's
// curl does not carry the MCP client's authorization header.
package transfer

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// TTL is how long an unused link stays valid, and how long the outcome of a
// used one can be read. Long enough for an agent to run the command it was
// given, short enough that a link seen later is useless.
const TTL = 10 * time.Minute

// maxPending bounds how many links may wait at once, so a caller that never
// uses them cannot grow the server's memory without end.
const maxPending = 256

// BaseHeader carries the address a caller reached the server at from the
// HTTP handler to the tools, which see request headers but not the request.
// Middleware sets it and drops any value a client sent.
const BaseHeader = "X-Kafka-Mcp-Transfer-Base"

// Download writes a file to w. It runs when the link is fetched, with the
// request's context, so a client that hangs up stops the read. Its result is
// what the status link reports afterwards.
type Download func(ctx context.Context, w io.Writer) (any, error)

// Upload reads a file from r. It runs when the link receives the file, with
// the request's context. Its result is returned to the uploader as JSON.
type Upload func(ctx context.Context, r io.Reader) (any, error)

// Ticket is a registered transfer as a caller sees it.
type Ticket struct {
	URL       string    `json:"url"`
	Method    string    `json:"method"`
	Command   string    `json:"command"`
	StatusURL string    `json:"status_url,omitempty"`
	ExpiresAt time.Time `json:"expires_at"`
	Note      string    `json:"note"`
}

type entry struct {
	filename    string
	contentType string
	download    Download
	upload      Upload
	expires     time.Time
}

// Status is the outcome of a used download link.
type Status struct {
	State  string `json:"state"`
	Error  string `json:"error,omitempty"`
	Result any    `json:"result,omitempty"`

	expires time.Time
}

// Hub holds the pending transfers of one server.
type Hub struct {
	path string

	mu      sync.Mutex
	pending map[string]*entry
	status  map[string]*Status
	now     func() time.Time
}

// NewHub returns a hub whose links are served under path, e.g.
// /kafka/mcp-files. Mount Handler at path + "/" and below.
func NewHub(path string) *Hub {
	return &Hub{
		path:    strings.TrimRight(path, "/"),
		pending: map[string]*entry{},
		status:  map[string]*Status{},
		now:     time.Now,
	}
}

// Path is where the hub's links live.
func (h *Hub) Path() string { return h.path }

// ErrUnavailable is returned when the server has no way to hand out links,
// such as over stdio.
var ErrUnavailable = errors.New("file transfer needs the HTTP transport: over stdio the server has no address the caller can fetch a link from")

// Base returns the address the caller reached the server at, as stamped by
// Middleware, or "" when it is unknown.
func Base(req *mcp.CallToolRequest) string {
	if req == nil || req.Extra == nil || req.Extra.Header == nil {
		return ""
	}

	return req.Extra.Header.Get(BaseHeader)
}

// Middleware stamps each request with the address it reached, so a link can
// be built for it. publicURL, when set, is used as is: a reverse proxy may
// show the server under an address it cannot work out. Otherwise the scheme
// and host come from the request, with X-Forwarded-Proto and -Host when a
// proxy set them. A client choosing these headers only changes the link it
// is handed itself.
func Middleware(publicURL string) func(http.Handler) http.Handler {
	publicURL = strings.TrimRight(publicURL, "/")

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			base := publicURL
			if base == "" {
				scheme := "http"
				if r.TLS != nil {
					scheme = "https"
				}
				if forwarded := first(r.Header.Get("X-Forwarded-Proto")); forwarded == "http" || forwarded == "https" {
					scheme = forwarded
				}
				host := r.Host
				if forwarded := first(r.Header.Get("X-Forwarded-Host")); forwarded != "" {
					host = forwarded
				}
				base = scheme + "://" + host
			}
			r.Header.Set(BaseHeader, base)
			next.ServeHTTP(w, r)
		})
	}
}

func first(list string) string {
	value, _, _ := strings.Cut(list, ",")
	return strings.TrimSpace(value)
}

// Offer registers a download and returns its ticket. base is Base(req).
// filename is the name the command saves to on the caller's machine.
func (h *Hub) Offer(base string, filename string, contentType string, download Download) (Ticket, error) {
	ticket, err := h.add(base, &entry{filename: filename, contentType: contentType, download: download})
	if err != nil {
		return Ticket{}, err
	}

	partial := shellQuote(filename + ".part")
	ticket.Method = http.MethodGet
	ticket.StatusURL = ticket.URL + "/status"
	// Saving to .part and renaming keeps a cut-short download from looking
	// like a finished file.
	ticket.Command = "curl -fsS -o " + partial + " " + shellQuote(ticket.URL) + " && mv " + partial + " " + shellQuote(filename)
	ticket.Note = "Run the command on the user's machine, in the directory the user wants the file in. " +
		"The data is read from Kafka while it downloads and is never stored on the server. " +
		"The link works once and expires at expires_at. If the command fails, fetch status_url for the reason."

	return ticket, nil
}

// Accept registers an upload and returns its ticket. filename names the
// caller's file in the suggested command.
func (h *Hub) Accept(base string, filename string, upload Upload) (Ticket, error) {
	ticket, err := h.add(base, &entry{filename: filename, upload: upload})
	if err != nil {
		return Ticket{}, err
	}

	ticket.Method = http.MethodPut
	ticket.Command = "curl -fsS -T " + shellQuote(filename) + " " + shellQuote(ticket.URL)
	ticket.Note = "Run the command on the user's machine, where the file is. " +
		"The file is read as it uploads and is never stored on the server; the command prints the result. " +
		"The link works once and expires at expires_at."

	return ticket, nil
}

func (h *Hub) add(base string, e *entry) (Ticket, error) {
	if h == nil || base == "" {
		return Ticket{}, ErrUnavailable
	}

	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return Ticket{}, fmt.Errorf("create a link: %w", err)
	}
	token := base64.RawURLEncoding.EncodeToString(raw)

	h.mu.Lock()
	defer h.mu.Unlock()

	now := h.now()
	h.sweep(now)
	if len(h.pending) >= maxPending {
		return Ticket{}, fmt.Errorf("%d links are already waiting to be used; use them or let them expire (%s) first", maxPending, TTL)
	}

	e.expires = now.Add(TTL)
	h.pending[token] = e

	return Ticket{URL: strings.TrimRight(base, "/") + h.path + "/" + token, ExpiresAt: e.expires.UTC()}, nil
}

// sweep drops expired links and outcomes. The caller holds h.mu.
func (h *Hub) sweep(now time.Time) {
	for token, e := range h.pending {
		if !now.Before(e.expires) {
			delete(h.pending, token)
		}
	}
	for token, s := range h.status {
		if !now.Before(s.expires) {
			delete(h.status, token)
		}
	}
}

// claim removes the transfer for token if method may use it, so a link works
// once and a wrong method does not spend it.
func (h *Hub) claim(token string, method string) (*entry, int, string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.sweep(h.now())

	e := h.pending[token]
	switch {
	case e == nil:
		return nil, http.StatusNotFound, "this link is unknown, already used or expired: ask for a new one"
	case e.download != nil && method != http.MethodGet:
		return nil, http.StatusMethodNotAllowed, "this link is a download: use GET"
	case e.upload != nil && method != http.MethodPut && method != http.MethodPost:
		return nil, http.StatusMethodNotAllowed, "this link takes an upload: send the file with PUT, e.g. curl -T"
	}

	delete(h.pending, token)
	if e.download != nil {
		h.status[token] = &Status{State: "running", expires: h.now().Add(TTL)}
	}

	return e, 0, ""
}

func (h *Hub) finish(token string, result any, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	s := &Status{State: "done", Result: result, expires: h.now().Add(TTL)}
	if err != nil {
		s.State, s.Error = "failed", err.Error()
	}
	h.status[token] = s
}

// Handler serves the links: GET for downloads, PUT or POST for uploads, and
// GET <link>/status for how a download ended.
func (h *Hub) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")

		rest, ok := strings.CutPrefix(r.URL.Path, h.path+"/")
		token, sub, _ := strings.Cut(rest, "/")
		if !ok || token == "" {
			http.NotFound(w, r)
			return
		}

		if sub == "status" && r.Method == http.MethodGet {
			h.mu.Lock()
			h.sweep(h.now())
			status := h.status[token]
			h.mu.Unlock()
			if status == nil {
				http.Error(w, "no download has used this link, or its outcome expired", http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(status)
			return
		}
		if sub != "" {
			http.NotFound(w, r)
			return
		}

		e, code, message := h.claim(token, r.Method)
		if e == nil {
			if code == http.StatusMethodNotAllowed {
				w.Header().Set("Allow", "GET, PUT, POST")
			}
			http.Error(w, message, code)
			return
		}

		if e.download != nil {
			w.Header().Set("Content-Type", e.contentType)
			w.Header().Set("Content-Disposition", "attachment; filename*=UTF-8''"+url.PathEscape(e.filename))
			result, err := e.download(r.Context(), w)
			h.finish(token, result, err)
			if err != nil {
				// Headers go out with the first byte, so a failure after that
				// can only cut the body short. Aborting the connection makes
				// the client report an error instead of keeping the file.
				panic(http.ErrAbortHandler)
			}
			return
		}

		result, err := e.upload(r.Context(), r.Body)
		w.Header().Set("Content-Type", "application/json")
		if err != nil {
			w.WriteHeader(http.StatusUnprocessableEntity)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		_ = json.NewEncoder(w).Encode(result)
	})
}

// shellQuote quotes s for a POSIX shell.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
