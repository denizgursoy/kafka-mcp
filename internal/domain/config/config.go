// Package config loads the server's own configuration: which clusters to talk
// to, how to authenticate to each, and what the server is allowed to do.
//
// Configuration is loaded by chu, with CONFIG_FILE selecting a YAML or JSON file.
//
// A deployment may serve several clusters. Each is served on its own HTTP
// path, so a session is bound to one cluster by the endpoint it connects to
// rather than by a parameter a caller could forget.
package config

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	mcors "github.com/rakunlabs/ada/middleware/cors"
	"github.com/rakunlabs/chu"
	"github.com/rakunlabs/chu/loader/loaderenv"
	"github.com/rakunlabs/chu/loader/loaderfile"

	_ "github.com/rakunlabs/chu/loader/external/loaderawssecrets"
	_ "github.com/rakunlabs/chu/loader/external/loaderawsssm"
	_ "github.com/rakunlabs/chu/loader/external/loaderazurekeyvault"
	_ "github.com/rakunlabs/chu/loader/external/loaderconsul"
	_ "github.com/rakunlabs/chu/loader/external/loadergcpparameter"
	_ "github.com/rakunlabs/chu/loader/external/loadergcpsecret"
	_ "github.com/rakunlabs/chu/loader/external/loadervault"
)

// SASL mechanisms the server can authenticate with.
const (
	MechanismPlain       = "plain"
	MechanismScramSHA256 = "scram-sha-256"
	MechanismScramSHA512 = "scram-sha-512"
	MechanismOAuth       = "oauthbearer"
)

// SASL is the identity the server connects as. Kafka ACLs are enforced
// against this principal, which is what allows two people running the same
// server to have different permissions.
type SASL struct {
	Mechanism string     `cfg:"mechanism"`
	User      string     `cfg:"user"`
	Zid       string     `cfg:"zid"`
	IsToken   bool       `cfg:"is_token"`
	OAuth     *SASLOAuth `cfg:"-"`

	// Password may be given literally, or as {env:VAR}, so that a config file
	// can be committed without carrying a secret.
	Password string `cfg:"password" log:"false"`

	// PasswordFile reads the secret from a file instead, which is how secrets
	// usually arrive in Kubernetes and systemd deployments.
	PasswordFile string `cfg:"password_file"`
}

// TLS controls transport encryption to the brokers.
type TLS struct {
	Enabled  bool   `cfg:"enabled"`
	CAFile   string `cfg:"ca_file"`
	CertFile string `cfg:"cert_file"`
	KeyFile  string `cfg:"key_file"`
}

// Security follows wkafka's TLS and ordered SASL configuration layout.
type Security struct {
	TLS  *TLS        `cfg:"tls"`
	SASL []SASLEntry `cfg:"sasl"`
}

type SASLEntry struct {
	Plain SASLPlain `cfg:"plain"`
	SCRAM SASLSCRAM `cfg:"scram"`
	OAuth SASLOAuth `cfg:"oauth"`
}

type SASLPlain struct {
	Enabled      bool   `cfg:"enabled"`
	Zid          string `cfg:"zid"`
	User         string `cfg:"user"`
	Pass         string `cfg:"pass" log:"false"`
	PasswordFile string `cfg:"password_file"`
}

type SASLSCRAM struct {
	Enabled      bool   `cfg:"enabled"`
	Algorithm    string `cfg:"algorithm"`
	Zid          string `cfg:"zid"`
	User         string `cfg:"user"`
	Pass         string `cfg:"pass" log:"false"`
	PasswordFile string `cfg:"password_file"`
	IsToken      bool   `cfg:"is_token"`
}

// HTTP controls where the server listens.
type HTTP struct {
	Address  string `cfg:"address" default:":8090"`
	BasePath string `cfg:"base_path"`

	// CORS is ada's own CORS configuration, filled straight from the config
	// file. Its `cfg` tags are the config keys, so the middleware gains an
	// option and this server gains it with it, without a mapping in between
	// that can quietly fall behind.
	CORS mcors.Cors `cfg:"cors"`
}

// DefaultCORS is the policy the server starts from, and what it keeps for
// every key the config file does not mention.
//
// A command-line MCP client sends no Origin header and none of this applies to
// it. A client running in a browser is another matter: the browser discards
// the response unless the server allows the origin, and tells the page almost
// nothing about why, so these values decide whether a browser-based client can
// talk to this server at all.
func DefaultCORS() mcors.Cors {
	return mcors.Cors{
		// Every origin, because the endpoints are otherwise unreachable from
		// a browser and this is a debugging tool. It is also the widest the
		// policy ever gets: an allowed origin can drive every tool with the
		// server's Kafka credentials, so a deployment that can reach a
		// cluster worth protecting should narrow it.
		AllowOrigins: []string{"*"},

		// The streamable HTTP transport posts requests, opens the event
		// stream with GET, and ends the session with DELETE.
		AllowMethods: []string{
			http.MethodGet,
			http.MethodPost,
			http.MethodDelete,
			http.MethodOptions,
		},

		// A header missing here fails the whole preflight rather than being
		// dropped, and the transport needs the two mcp- ones from the second
		// request onwards.
		AllowHeaders: []string{
			"content-type",
			"accept",
			"authorization",
			"cache-control",
			"last-event-id",
			"mcp-session-id",
			"mcp-protocol-version",
		},

		// The session id arrives on the initialize response, and a page that
		// cannot read it cannot make a second call.
		ExposeHeaders: []string{"Mcp-Session-Id"},

		// Answers Chrome's Private Network Access preflight, which a page on
		// a public address must pass before it may reach a server on a
		// private or loopback address. On by default because that is the
		// usual shape of a browser client here, and the alternative failure
		// is a browser error with no server-side trace.
		AllowPrivateNetwork: true,

		MaxAge: 600,
	}
}

// cluster holds a cluster's input before validation and normalization.
type cluster struct {
	Security *Security       `cfg:"security"`
	Brokers  []string        `cfg:"brokers"`
	ReadOnly bool            `cfg:"read_only"`
	TLS      *TLS            `cfg:"tls"`
	SASL     *SASL           `cfg:"sasl"`
	Tools    map[string]bool `cfg:"tools"`
}

// endpoint is the policy and HTTP route applied to one view of a cluster.
// Connection details deliberately stay in cluster so several endpoints can
// reuse one Kafka client without repeating credentials.
type endpoint struct {
	Cluster     string          `cfg:"cluster"`
	Path        string          `cfg:"path"`
	Description string          `cfg:"description"`
	ReadOnly    bool            `cfg:"read_only"`
	Tools       map[string]bool `cfg:"tools"`
}

// file mirrors the whole config file.
type file struct {
	HTTP      HTTP                 `cfg:"http"`
	OutputDir string               `cfg:"output_dir"`
	Clusters  map[string]*cluster  `cfg:"clusters"`
	Endpoints map[string]*endpoint `cfg:"endpoints"`
}

// Cluster is one Kafka cluster the server can serve.
type Cluster struct {
	// Name is the key from the config file and is reported by cluster-aware
	// tools, so it is carried here rather than left as only a map key.
	Name string `cfg:"name"`

	Brokers  []string `cfg:"brokers"`
	ReadOnly bool     `cfg:"read_only"`
	TLS      *TLS     `cfg:"tls"`

	// SASL holds every enabled mechanism in configured preference order, so
	// both config forms end up as the same thing: the legacy single `sasl`
	// block is a list of one, and `security.sasl` is the list it declares.
	// franz-go is handed all of them and settles on one the broker offers.
	SASL []*SASL `cfg:"sasl"`

	// Tools is the legacy cluster-level tool policy. New configurations put
	// this on Endpoint; retaining it here keeps old files safe while migrating.
	//
	//	tools:
	//	  create_topic: false
	//
	// A tool the map does not mention stays on, so a deployment states only
	// what it wants to withhold rather than having to list the whole set and
	// silently losing whatever is added later.
	//
	// This is a narrowing, never a widening: a tool that read_only already
	// withholds is not brought back by setting it to true here. The names are
	// checked at startup against the tools that exist, because a typo that
	// quietly left a tool enabled would be the one failure mode worth having
	// this for.
	Tools map[string]bool `cfg:"tools"`
}

// Endpoint is one MCP view of a Kafka cluster. It owns the route and policy;
// the referenced Cluster owns brokers and authentication. More than one
// endpoint may therefore expose the same connection with different powers.
type Endpoint struct {
	Name        string
	Cluster     string
	Path        string
	Description string
	ReadOnly    bool
	Tools       map[string]bool
}

// ToolEnabled reports whether this endpoint should expose a tool.
func (e *Endpoint) ToolEnabled(name string) bool {
	enabled, listed := e.Tools[name]

	return !listed || enabled
}

// DisabledTools lists the endpoint's explicitly withheld tools in stable
// order for startup logs and diagnostics.
func (e *Endpoint) DisabledTools() []string {
	disabled := make([]string, 0, len(e.Tools))
	for name, enabled := range e.Tools {
		if !enabled {
			disabled = append(disabled, name)
		}
	}
	sort.Strings(disabled)

	return disabled
}

// ToolEnabled reports the legacy cluster-level tool policy.
//
// Absence means enabled: the map lists exceptions, so a cluster that says
// nothing about a tool gets it.
func (c *Cluster) ToolEnabled(name string) bool {
	enabled, listed := c.Tools[name]

	return !listed || enabled
}

// DisabledTools lists tools withheld by the legacy cluster-level policy.
func (c *Cluster) DisabledTools() []string {
	disabled := make([]string, 0, len(c.Tools))

	for name, enabled := range c.Tools {
		if !enabled {
			disabled = append(disabled, name)
		}
	}

	sort.Strings(disabled)

	return disabled
}

// Config is the effective configuration the server runs with.
type Config struct {
	HTTP      HTTP
	OutputDir string

	// Clusters is keyed by cluster name.
	Clusters map[string]*Cluster

	// Endpoints is keyed by endpoint name. Every entry binds an exact HTTP
	// path and an exposure policy to one cluster.
	Endpoints map[string]*Endpoint

	// Path is the file this came from.
	Path string
}

// ClusterNames returns every configured cluster name, sorted.
//
// Go map order is random, so anything reported to a caller is sorted here
// rather than left to vary between calls.
func (c *Config) ClusterNames() []string {
	names := make([]string, 0, len(c.Clusters))

	for name := range c.Clusters {
		names = append(names, name)
	}

	sort.Strings(names)

	return names
}

// EndpointNames returns every configured endpoint name, sorted.
func (c *Config) EndpointNames() []string {
	names := make([]string, 0, len(c.Endpoints))
	for name := range c.Endpoints {
		names = append(names, name)
	}
	sort.Strings(names)

	return names
}

// Load uses chu's default, file, HTTP and environment loaders.
func Load(ctx context.Context) (*Config, error) {
	var parsed file

	// Seeded before loading, because chu merges the file over the struct it
	// is given rather than replacing it. A key the file omits keeps its
	// default, a key it sets wins, and `allow_private_network: false` is
	// still distinguishable from the field being absent.
	parsed.HTTP.CORS = DefaultCORS()

	configFolders := []string{"/etc"}
	if userConfigDir, err := os.UserConfigDir(); err == nil {
		configFolders = append([]string{filepath.Join(userConfigDir, "kafka-mcp")}, configFolders...)
	}

	if err := chu.Load(ctx, "kafka-mcp", &parsed,
		chu.WithLoaderOption(loaderfile.New(loaderfile.WithFolders(configFolders...))),
		chu.WithLoaderOption(loaderenv.New(loaderenv.WithPrefix("KAFKA_MCP_"))),
	); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	if len(parsed.Clusters) == 0 {
		return nil, fmt.Errorf(
			"config: at least one cluster is required; set CONFIG_FILE or KAFKA_MCP_CLUSTERS in the environment")
	}

	cfg := &Config{
		HTTP:      parsed.HTTP,
		OutputDir: parsed.OutputDir,
		Clusters:  make(map[string]*Cluster, len(parsed.Clusters)),
		Endpoints: make(map[string]*Endpoint),
	}

	basePath, err := normalizeBasePath(cfg.HTTP.BasePath)
	if err != nil {
		return nil, err
	}
	cfg.HTTP.BasePath = basePath

	// Exports still need somewhere to go, and the temp directory needs no
	// configuration to be usable.
	if cfg.OutputDir == "" {
		cfg.OutputDir = os.TempDir()
	}

	for name, parsedCluster := range parsed.Clusters {
		resolved, err := resolveCluster(name, parsedCluster)
		if err != nil {
			return nil, err
		}

		cfg.Clusters[name] = resolved
	}

	if len(parsed.Endpoints) == 0 {
		// Compatibility with the original format, where each cluster was also
		// its endpoint and was reached at /mcp/<cluster>.
		for name, cluster := range cfg.Clusters {
			cfg.Endpoints[name] = &Endpoint{
				Name:     name,
				Cluster:  name,
				Path:     "/mcp/" + name,
				ReadOnly: cluster.ReadOnly,
				Tools:    copyToolPolicy(cluster.Tools),
			}
		}
	} else {
		paths := make(map[string]string, len(parsed.Endpoints))
		for name, parsedEndpoint := range parsed.Endpoints {
			resolved, err := resolveEndpoint(name, parsedEndpoint, cfg.Clusters)
			if err != nil {
				return nil, err
			}
			if previous, exists := paths[resolved.Path]; exists {
				return nil, fmt.Errorf(
					"endpoints %q and %q use the same path %q", previous, name, resolved.Path)
			}
			paths[resolved.Path] = name
			cfg.Endpoints[name] = resolved
		}
	}

	return cfg, nil
}

func resolveEndpoint(name string, parsed *endpoint, clusters map[string]*Cluster) (*Endpoint, error) {
	if parsed == nil {
		return nil, fmt.Errorf("endpoint %q must not be null", name)
	}
	if strings.TrimSpace(name) == "" {
		return nil, fmt.Errorf("config: an endpoint name must not be empty")
	}

	cluster := clusters[parsed.Cluster]
	if cluster == nil {
		return nil, fmt.Errorf("endpoint %q references unknown cluster %q", name, parsed.Cluster)
	}

	endpointPath, err := normalizeEndpointPath(parsed.Path, name)
	if err != nil {
		return nil, err
	}

	// Legacy cluster-level policy remains a lower bound during migration: an
	// endpoint may narrow it, but may never turn an old protection back on.
	tools := copyToolPolicy(cluster.Tools)
	if len(parsed.Tools) > 0 && tools == nil {
		tools = make(map[string]bool, len(parsed.Tools))
	}
	for tool, enabled := range parsed.Tools {
		if existing, listed := tools[tool]; listed && !existing {
			continue
		}
		tools[tool] = enabled
	}

	return &Endpoint{
		Name:        name,
		Cluster:     parsed.Cluster,
		Path:        endpointPath,
		Description: strings.TrimSpace(parsed.Description),
		ReadOnly:    cluster.ReadOnly || parsed.ReadOnly,
		Tools:       tools,
	}, nil
}

func normalizeEndpointPath(value string, name string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "/mcp/" + name, nil
	}
	if strings.ContainsAny(value, "?#") {
		return "", fmt.Errorf("endpoint %q path must contain only a URL path, not a query or fragment", name)
	}

	cleaned := path.Clean("/" + strings.TrimLeft(value, "/"))
	if cleaned == "/" {
		return "", fmt.Errorf("endpoint %q path must not be the HTTP root", name)
	}
	if cleaned == "/healthz" {
		return "", fmt.Errorf("endpoint %q path %q conflicts with the health route", name, cleaned)
	}

	return cleaned, nil
}

func copyToolPolicy(source map[string]bool) map[string]bool {
	if len(source) == 0 {
		return nil
	}
	copy := make(map[string]bool, len(source))
	for name, enabled := range source {
		copy[name] = enabled
	}

	return copy
}

// normalizeBasePath returns either an empty string for the HTTP root or an
// absolute path without a trailing slash. Keeping one representation makes it
// safe for the server to append /mcp and /healthz without doubled slashes.
func normalizeBasePath(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" || value == "/" {
		return "", nil
	}
	if strings.ContainsAny(value, "?#") {
		return "", fmt.Errorf("config: http.base_path must contain only a URL path, not a query or fragment")
	}

	cleaned := path.Clean("/" + strings.TrimLeft(value, "/"))
	if cleaned == "/" {
		return "", nil
	}

	return cleaned, nil
}

func resolveCluster(name string, parsed *cluster) (*Cluster, error) {
	if parsed == nil {
		return nil, fmt.Errorf("cluster %q must not be null", name)
	}
	if strings.TrimSpace(name) == "" {
		return nil, fmt.Errorf("config: a cluster name must not be empty")
	}

	// The name becomes a URL path segment, so a name with a slash would make
	// the cluster unreachable rather than merely odd.
	if strings.ContainsAny(name, "/ ") {
		return nil, fmt.Errorf(
			"cluster name %q must not contain a slash or a space, because it names the endpoint path",
			name)
	}

	resolved := &Cluster{
		Name:     name,
		Brokers:  normalizeBrokers(parsed.Brokers),
		ReadOnly: parsed.ReadOnly,
		TLS:      parsed.TLS,
		Tools:    parsed.Tools,
	}

	if len(resolved.Brokers) == 0 {
		return nil, fmt.Errorf("cluster %q needs a broker", name)
	}

	if parsed.Security != nil {
		if parsed.TLS != nil || parsed.SASL != nil {
			return nil, fmt.Errorf("cluster %q: security cannot be combined with legacy tls or sasl", name)
		}
		resolved.TLS = parsed.Security.TLS
		for i, entry := range parsed.Security.SASL {
			enabled := 0
			for _, active := range []bool{entry.Plain.Enabled, entry.SCRAM.Enabled, entry.OAuth.Enabled} {
				if active {
					enabled++
				}
			}
			if enabled > 1 {
				return nil, fmt.Errorf("cluster %q security.sasl[%d]: enable only one of plain, scram or oauth", name, i)
			}
			var auth *SASL
			if p := entry.Plain; p.Enabled {
				auth = &SASL{Mechanism: MechanismPlain, User: p.User, Password: p.Pass, PasswordFile: p.PasswordFile, Zid: p.Zid}
			}
			if p := entry.SCRAM; p.Enabled {
				auth = &SASL{Mechanism: p.Algorithm, User: p.User, Password: p.Pass, PasswordFile: p.PasswordFile, Zid: p.Zid, IsToken: p.IsToken}
				if strings.EqualFold(strings.TrimSpace(p.Algorithm), MechanismPlain) {
					return nil, fmt.Errorf("cluster %q security.sasl[%d]: scram requires SCRAM-SHA-256 or SCRAM-SHA-512", name, i)
				}
			}
			if auth == nil {
				if p := entry.OAuth; p.Enabled {
					if err := resolveOAuth(&p, name); err != nil {
						return nil, fmt.Errorf("cluster %q security.sasl[%d]: %w", name, i, err)
					}
					auth = &SASL{Mechanism: MechanismOAuth, Zid: p.Zid, OAuth: &p}
				}
			}
			if auth == nil {
				continue
			}
			if auth.OAuth == nil {
				if err := resolveSASL(auth, name); err != nil {
					return nil, fmt.Errorf("security.sasl[%d]: %w", i, err)
				}
			}
			for _, previous := range resolved.SASL {
				if previous.Mechanism == auth.Mechanism {
					return nil, fmt.Errorf("cluster %q: duplicate sasl mechanism %q", name, auth.Mechanism)
				}
			}
			resolved.SASL = append(resolved.SASL, auth)
		}
	} else if parsed.SASL != nil {
		if err := resolveSASL(parsed.SASL, name); err != nil {
			return nil, err
		}

		resolved.SASL = []*SASL{parsed.SASL}
	}
	if t := resolved.TLS; t != nil && t.Enabled && (t.CertFile == "") != (t.KeyFile == "") {
		return nil, fmt.Errorf("cluster %q: tls cert_file and key_file must be supplied together", name)
	}

	return resolved, nil
}

// resolveSASL validates the mechanism and turns whichever password form was
// used into the actual secret.
func resolveSASL(auth *SASL, name string) error {
	if auth == nil {
		return nil
	}

	mechanism := strings.ToLower(strings.TrimSpace(auth.Mechanism))

	switch mechanism {
	case MechanismPlain, MechanismScramSHA256, MechanismScramSHA512:
		auth.Mechanism = mechanism
	case "":
		return fmt.Errorf(
			"config: cluster %q sasl needs a mechanism, one of %s, %s or %s",
			name, MechanismPlain, MechanismScramSHA256, MechanismScramSHA512)
	default:
		return fmt.Errorf(
			"config: cluster %q has unsupported sasl mechanism %q, must be one of %s, %s or %s",
			name, auth.Mechanism,
			MechanismPlain, MechanismScramSHA256, MechanismScramSHA512)
	}

	if auth.User == "" {
		return fmt.Errorf("config: cluster %q sasl needs a user", name)
	}

	password, err := resolvePassword(auth, name)
	if err != nil {
		return err
	}

	// Connecting anonymously when credentials were configured would silently
	// drop the identity that ACLs are enforced against.
	if password == "" {
		return fmt.Errorf(
			"config: cluster %q sasl user %q has no password: set password, password_file, or use {env:VAR}",
			name, auth.User)
	}

	auth.Password = password
	auth.PasswordFile = ""

	return nil
}

func resolvePassword(sasl *SASL, name string) (string, error) {
	if sasl.PasswordFile != "" {
		if sasl.Password != "" {
			return "", fmt.Errorf(
				"config: cluster %q sets both password and password_file for sasl, which is ambiguous",
				name)
		}

		contents, err := os.ReadFile(sasl.PasswordFile)
		if err != nil {
			return "", fmt.Errorf("config: cluster %q read password_file: %w", name, err)
		}

		// Secret files almost always end in a newline, and Kafka would treat
		// it as part of the password.
		return strings.TrimSpace(string(contents)), nil
	}

	return interpolate(sasl.Password, name)
}

// envPlaceholder matches {env:VAR}, the form used to keep a secret out of the
// config file itself.
var envPlaceholder = regexp.MustCompile(`^\{env:([A-Za-z_][A-Za-z0-9_]*)\}$`)

func interpolate(value string, name string) (string, error) {
	match := envPlaceholder.FindStringSubmatch(value)
	if match == nil {
		return value, nil
	}

	variable := match[1]

	resolved := os.Getenv(variable)
	if resolved == "" {
		return "", fmt.Errorf(
			"config: cluster %q references environment variable %s, which is not set",
			name, variable)
	}

	return resolved, nil
}

func normalizeBrokers(values []string) []string {
	brokers := make([]string, 0, len(values))

	for _, broker := range values {
		if trimmed := strings.TrimSpace(broker); trimmed != "" {
			brokers = append(brokers, trimmed)
		}
	}

	return brokers
}

// SASLIdentity is the non-secret portion of one configured SASL option.
type SASLIdentity struct {
	Mechanism string `json:"mechanism"`
	User      string `json:"user,omitempty"`
	Zid       string `json:"zid,omitempty"`
	ClientID  string `json:"client_id,omitempty"`
}

// AuthenticationOptions reports configured identities in preference order,
// not the mechanism negotiated by any individual broker connection.
func (c *Cluster) AuthenticationOptions() []SASLIdentity {
	identities := make([]SASLIdentity, 0, len(c.SASL))
	for _, auth := range c.SASL {
		identity := SASLIdentity{Mechanism: auth.Mechanism, User: auth.User, Zid: auth.Zid}
		if auth.OAuth != nil {
			identity.ClientID = auth.OAuth.ClientID
		}
		identities = append(identities, identity)
	}
	return identities
}

// Describe renders one cluster for reporting without exposing secrets.
func (c *Cluster) Describe() map[string]any {
	described := map[string]any{
		"name":      c.Name,
		"brokers":   c.Brokers,
		"read_only": c.ReadOnly,
		"tls":       c.TLS != nil && c.TLS.Enabled,
	}

	identities := c.AuthenticationOptions()
	if len(identities) > 0 {
		// The mechanism and principal are what an operator needs to reason
		// about ACLs. The password is never useful to a caller.
		described["authentication"] = identities[0].Mechanism
		described["sasl_user"] = identities[0].User
		described["sasl_options"] = identities
	} else {
		described["authentication"] = "none"
	}

	return described
}
