// Package transfer moves files between an MCP caller's own machine and Kafka
// without the server ever holding them on disk.
//
// It lives in internal/domain because several tools share it: search_messages
// and transform_messages hand out downloads, and produce_message an upload.
//
// A tool cannot write to the caller's machine: an MCP result goes to the
// model, not to a file. So a tool returns a link, and the caller's agent
// fetches it there, e.g. with curl. The work runs when the link is used,
// streaming from Kafka into the response or from the request into Kafka.
//
// The link carries the work itself, sealed with AES-GCM: which tool, with
// which arguments, until when. The server keeps nothing between handing a
// link out and its use, so any instance behind a load balancer can open a
// link another made, as long as they share the key. Sealing means a link can
// be neither read, which would show its script in proxy logs, nor changed or
// made up, which would run a search the caller was never offered.
//
// Over stdio the server runs on the caller's machine, so a local hub skips
// the link and reads or writes the file there directly.
package transfer

import (
	"bytes"
	"compress/flate"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
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

// TTL is how long a link stays valid. Long enough for an agent to run the
// command it was given, short enough that a link seen later is useless.
const TTL = 10 * time.Minute

// maxLink bounds a link's length. Many proxies refuse request lines much
// past 8 KiB, and a link that is refused there fails far from its cause.
const maxLink = 6 << 10

// BaseHeader carries the address a caller reached the server at from the
// HTTP handler to the tools, which see request headers but not the request.
// Middleware sets it and drops any value a client sent.
const BaseHeader = "X-Kafka-Mcp-Transfer-Base"

// Download writes a file to w. It runs when the link is fetched, with the
// request's context, so a client that hangs up stops the read. Its result is
// returned to the tool when a local hub ran it; a link has nowhere to put it.
type Download func(ctx context.Context, w io.Writer) (any, error)

// Upload reads a file from r. It runs when the link receives the file, with
// the request's context. Its result is returned to the uploader as JSON.
type Upload func(ctx context.Context, r io.Reader) (any, error)

// Opener turns a link's sealed arguments back into the work, re-checking
// them as a fresh call would, because the endpoint's policy may have changed
// since the link was made. It returns a Download or an Upload.
type Opener func(ctx context.Context, args json.RawMessage) (any, error)

// Ticket is a transfer as a caller sees it: a link to fetch, or, from a
// local hub, the file that was already written or read.
type Ticket struct {
	URL       string     `json:"url,omitempty"`
	Method    string     `json:"method"`
	Command   string     `json:"command,omitempty"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
	// Path is the file a local hub wrote or read.
	Path string `json:"path,omitempty"`
	// Result is what a local transfer returned; tools lift it into their
	// own output.
	Result any    `json:"result,omitempty"`
	Note   string `json:"note"`
}

// MethodLocal marks a ticket whose file was handled on this machine.
const MethodLocal = "local"

// sealed is what a link carries.
type sealed struct {
	// Kind names the Opener; Args are its arguments.
	Kind string          `json:"k"`
	Args json.RawMessage `json:"a"`
	// Filename and ContentType shape a download's response.
	Filename    string `json:"f,omitempty"`
	ContentType string `json:"c,omitempty"`
	Upload      bool   `json:"u,omitempty"`
	// Expires is Unix seconds.
	Expires int64 `json:"e"`
}

// Hub hands out and serves the links of one endpoint.
type Hub struct {
	path string
	aead cipher.AEAD
	// dir, when set, makes this a local hub: files are written and read
	// there instead of through links.
	dir string

	mu      sync.RWMutex
	openers map[string]Opener

	now func() time.Time
}

// NewHub returns a hub whose links are served under path, e.g.
// /kafka/mcp-files, sealed with a key derived from secret. Every instance
// serving the same links must use the same secret; an empty one makes a
// random key, so links work only on this process. Mount Handler at path + "/"
// and below.
func NewHub(path string, secret []byte) (*Hub, error) {
	if len(secret) == 0 {
		secret = make([]byte, 32)
		if _, err := rand.Read(secret); err != nil {
			return nil, fmt.Errorf("transfer: make a link key: %w", err)
		}
	}

	path = strings.TrimRight(path, "/")
	// The path is in the derivation, so a link made for one endpoint does
	// not open on another that shares the secret.
	key, err := hkdf.Key(sha256.New, secret, nil, "kafka-mcp transfer link "+path, 32)
	if err != nil {
		return nil, fmt.Errorf("transfer: derive the link key: %w", err)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("transfer: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("transfer: %w", err)
	}

	return &Hub{path: path, aead: aead, openers: map[string]Opener{}, now: time.Now}, nil
}

// NewLocalHub returns a hub for a server on the caller's own machine, such
// as over stdio: downloads are saved in dir and uploads read from it.
func NewLocalHub(dir string) *Hub {
	return &Hub{dir: dir, openers: map[string]Opener{}, now: time.Now}
}

// Path is where the hub's links live.
func (h *Hub) Path() string { return h.path }

// Local reports whether the hub handles files on this machine.
func (h *Hub) Local() bool { return h != nil && h.dir != "" }

// Register names the Opener a tool's links are opened with. A tool
// registers once, when it is added to the server.
func (h *Hub) Register(kind string, open Opener) {
	if h == nil {
		return
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	h.openers[kind] = open
}

// ErrUnavailable is returned when the server has no way to hand out links.
var ErrUnavailable = errors.New("file transfer is not set up for this server: it needs the HTTP transport, or stdio on the user's own machine")

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

// Offer returns a download link for kind's work with args. base is
// Base(req); filename is the name the command saves to on the caller's
// machine. A local hub opens and runs the work now, with ctx, and saves the
// file itself.
func (h *Hub) Offer(ctx context.Context, base, kind string, args any, filename, contentType string) (Ticket, error) {
	if h.Local() {
		return h.saveNow(ctx, kind, args, filename)
	}

	ticket, err := h.seal(base, kind, args, sealed{Filename: filename, ContentType: contentType})
	if err != nil {
		return Ticket{}, err
	}

	partial := shellQuote(filename + ".part")
	ticket.Method = http.MethodGet
	// Saving to .part and renaming keeps a cut-short download from looking
	// like a finished file.
	ticket.Command = "curl -fsS -o " + partial + " " + shellQuote(ticket.URL) + " && mv " + partial + " " + shellQuote(filename)
	ticket.Note = "Run the command on the user's machine, in the directory the user wants the file in. " +
		"The data is read from Kafka while it downloads and is never stored on the server. " +
		"The link expires at expires_at; treat it as a secret until then. If curl fails, its output says why."

	return ticket, nil
}

// Accept returns an upload link for kind's work with args. filename names
// the caller's file in the suggested command. A local hub opens the work and
// reads the file now, with ctx.
func (h *Hub) Accept(ctx context.Context, base, kind string, args any, filename string) (Ticket, error) {
	if h.Local() {
		return h.readNow(ctx, kind, args, filename)
	}

	ticket, err := h.seal(base, kind, args, sealed{Filename: filename, Upload: true})
	if err != nil {
		return Ticket{}, err
	}

	ticket.Method = http.MethodPut
	ticket.Command = "curl -fsS -T " + shellQuote(filename) + " " + shellQuote(ticket.URL)
	ticket.Note = "Run the command on the user's machine, where the file is. " +
		"The file is read as it uploads and is never stored on the server; the command prints the result. " +
		"The link expires at expires_at; treat it as a secret until then, and run it once: each use sends the file again."

	return ticket, nil
}

func (h *Hub) seal(base, kind string, args any, s sealed) (Ticket, error) {
	if h == nil || h.aead == nil || base == "" {
		return Ticket{}, ErrUnavailable
	}

	// A link nobody here can open would only fail later, as a curl error.
	h.mu.RLock()
	_, known := h.openers[kind]
	h.mu.RUnlock()
	if !known {
		return Ticket{}, fmt.Errorf("transfer: %s links are not registered on this hub", kind)
	}

	raw, err := json.Marshal(args)
	if err != nil {
		return Ticket{}, fmt.Errorf("transfer: encode the link: %w", err)
	}

	expires := h.now().Add(TTL).UTC().Truncate(time.Second)
	s.Kind, s.Args, s.Expires = kind, raw, expires.Unix()

	plain, err := json.Marshal(s)
	if err != nil {
		return Ticket{}, fmt.Errorf("transfer: encode the link: %w", err)
	}

	var packed bytes.Buffer
	zw, _ := flate.NewWriter(&packed, flate.BestCompression)
	_, _ = zw.Write(plain)
	_ = zw.Close()

	nonce := make([]byte, h.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return Ticket{}, fmt.Errorf("transfer: make a link: %w", err)
	}
	box := h.aead.Seal(nonce, nonce, packed.Bytes(), nil)

	link := strings.TrimRight(base, "/") + h.path + "/" + base64.RawURLEncoding.EncodeToString(box)
	if len(link) > maxLink {
		return Ticket{}, fmt.Errorf("the request is too large to fit in a link (%d bytes, at most %d): shorten the script or transform", len(link), maxLink)
	}

	return Ticket{URL: link, ExpiresAt: &expires}, nil
}

// open reads a link back. It fails alike for a link that is malformed,
// forged, sealed with another key or for another endpoint, so a caller
// learns nothing about why.
func (h *Hub) open(token string) (*sealed, error) {
	box, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(box) < h.aead.NonceSize() {
		return nil, errBadLink
	}

	packed, err := h.aead.Open(nil, box[:h.aead.NonceSize()], box[h.aead.NonceSize():], nil)
	if err != nil {
		return nil, errBadLink
	}

	plain, err := io.ReadAll(io.LimitReader(flate.NewReader(bytes.NewReader(packed)), 1<<20))
	if err != nil {
		return nil, errBadLink
	}

	var s sealed
	if err := json.Unmarshal(plain, &s); err != nil {
		return nil, errBadLink
	}

	if !h.now().Before(time.Unix(s.Expires, 0)) {
		return nil, errExpired
	}

	return &s, nil
}

var (
	errBadLink = errors.New("this link is not valid here: ask for a new one")
	errExpired = errors.New("this link has expired: ask for a new one")
)

// work resolves what a link runs.
func (h *Hub) work(ctx context.Context, kind string, args json.RawMessage) (any, error) {
	h.mu.RLock()
	open := h.openers[kind]
	h.mu.RUnlock()

	if open == nil {
		return nil, fmt.Errorf("this server does not offer %s links", kind)
	}

	return open(ctx, args)
}

// Handler serves the links: GET for downloads, PUT or POST for uploads.
func (h *Hub) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")

		token, ok := strings.CutPrefix(r.URL.Path, h.path+"/")
		if !ok || token == "" || strings.Contains(token, "/") || h.aead == nil {
			http.NotFound(w, r)
			return
		}

		link, err := h.open(token)
		if err != nil {
			code := http.StatusNotFound
			if errors.Is(err, errExpired) {
				code = http.StatusGone
			}
			http.Error(w, err.Error(), code)
			return
		}

		switch {
		case !link.Upload && r.Method != http.MethodGet:
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "this link is a download: use GET", http.StatusMethodNotAllowed)
			return
		case link.Upload && r.Method != http.MethodPut && r.Method != http.MethodPost:
			w.Header().Set("Allow", "PUT, POST")
			http.Error(w, "this link takes an upload: send the file with PUT, e.g. curl -T", http.StatusMethodNotAllowed)
			return
		}

		// The policy is checked again now, so a link made before an
		// endpoint turned read-only cannot write after it.
		work, err := h.work(r.Context(), link.Kind, link.Args)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnprocessableEntity)
			return
		}

		switch run := work.(type) {
		case Download:
			if link.Upload {
				http.Error(w, "this link takes an upload", http.StatusMethodNotAllowed)
				return
			}
			serveDownload(w, r, link, run)
		case Upload:
			if !link.Upload {
				http.Error(w, "this link is a download: use GET", http.StatusMethodNotAllowed)
				return
			}
			result, err := run(r.Context(), r.Body)
			w.Header().Set("Content-Type", "application/json")
			if err != nil {
				w.WriteHeader(http.StatusUnprocessableEntity)
				_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
				return
			}
			_ = json.NewEncoder(w).Encode(result)
		default:
			http.Error(w, "this link has no work to run", http.StatusInternalServerError)
		}
	})
}

// serveDownload holds the status until the first byte, so a failure before
// any data, the common case of a bad topic or a refused policy, is a plain
// HTTP error curl -f reports with its reason. After the first byte a failure
// can only break the connection, which curl -f reports too.
func serveDownload(w http.ResponseWriter, r *http.Request, link *sealed, run Download) {
	out := &lazyWriter{w: w, start: func() {
		w.Header().Set("Content-Type", link.ContentType)
		w.Header().Set("Content-Disposition", "attachment; filename*=UTF-8''"+url.PathEscape(link.Filename))
		w.WriteHeader(http.StatusOK)
	}}

	_, err := run(r.Context(), out)
	if err == nil {
		out.begin()
		return
	}

	if !out.started {
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}

	panic(http.ErrAbortHandler)
}

// lazyWriter writes the response head with its first byte.
type lazyWriter struct {
	w       http.ResponseWriter
	start   func()
	started bool
}

func (l *lazyWriter) begin() {
	if !l.started {
		l.started = true
		l.start()
	}
}

func (l *lazyWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	l.begin()

	return l.w.Write(p)
}

// shellQuote quotes s for a POSIX shell.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
