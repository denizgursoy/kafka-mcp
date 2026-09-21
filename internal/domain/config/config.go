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
	"regexp"
	"sort"
	"strings"

	mcors "github.com/rakunlabs/ada/middleware/cors"
	"github.com/rakunlabs/chu"
	"github.com/rakunlabs/chu/loader/loaderenv"
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
	Address string `cfg:"address" default:":8080"`

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

// cluster holds a cluster's input before splitting the comma-separated brokers.
type cluster struct {
	Security *Security `cfg:"security"`
	Broker   string    `cfg:"broker"`
	ReadOnly bool      `cfg:"read_only"`
	TLS      *TLS      `cfg:"tls"`
	SASL     *SASL     `cfg:"sasl"`
}

// file mirrors the whole config file.
type file struct {
	HTTP      HTTP                `cfg:"http"`
	OutputDir string              `cfg:"output_dir"`
	Clusters  map[string]*cluster `cfg:"clusters"`
}

// Cluster is one Kafka cluster the server can serve.
type Cluster struct {
	// Name is the key from the config file. It names the HTTP path the
	// cluster is served on and is reported by list_clusters, so it is carried
	// here rather than left as only a map key.
	Name string `cfg:"name"`

	Brokers  []string `cfg:"brokers"`
	ReadOnly bool     `cfg:"read_only"`
	TLS      *TLS     `cfg:"tls"`

	// SASL holds every enabled mechanism in configured preference order, so
	// both config forms end up as the same thing: the legacy single `sasl`
	// block is a list of one, and `security.sasl` is the list it declares.
	// franz-go is handed all of them and settles on one the broker offers.
	SASL []*SASL `cfg:"sasl"`
}

// Config is the effective configuration the server runs with.
type Config struct {
	HTTP      HTTP
	OutputDir string

	// Clusters is keyed by cluster name.
	Clusters map[string]*Cluster

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

// Load uses chu's default, file, HTTP and environment loaders.
func Load(ctx context.Context) (*Config, error) {
	var parsed file

	// Seeded before loading, because chu merges the file over the struct it
	// is given rather than replacing it. A key the file omits keeps its
	// default, a key it sets wins, and `allow_private_network: false` is
	// still distinguishable from the field being absent.
	parsed.HTTP.CORS = DefaultCORS()

	if err := chu.Load(ctx, "kafka-mcp", &parsed,
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
	}

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

	return cfg, nil
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
		Brokers:  splitBrokers(parsed.Broker),
		ReadOnly: parsed.ReadOnly,
		TLS:      parsed.TLS,
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

func splitBrokers(value string) []string {
	brokers := make([]string, 0, 1)

	for _, broker := range strings.Split(value, ",") {
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
