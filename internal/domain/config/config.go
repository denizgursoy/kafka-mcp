// Package config loads the server's own configuration: which clusters to talk
// to, how to authenticate to each, and what the server is allowed to do.
//
// Configuration comes from a JSON file named by KAFKA_MCP_CONFIG, and from
// nowhere else. One file describes a whole deployment, so there is no way for
// a stale environment variable to point the server at a cluster nobody
// intended.
//
// A deployment may serve several clusters. Each is served on its own HTTP
// path, so a session is bound to one cluster by the endpoint it connects to
// rather than by a parameter a caller could forget.
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
)

// PathEnv names the config file to load. It is the only environment variable
// the server reads, because it needs some way to find the file in the first
// place.
const PathEnv = "KAFKA_MCP_CONFIG"

// DefaultAddress is where the server listens when the config does not say.
const DefaultAddress = ":8080"

// SASL mechanisms the server can authenticate with.
const (
	MechanismPlain       = "plain"
	MechanismScramSHA256 = "scram-sha-256"
	MechanismScramSHA512 = "scram-sha-512"
)

// SASL is the identity the server connects as. Kafka ACLs are enforced
// against this principal, which is what allows two people running the same
// server to have different permissions.
type SASL struct {
	Mechanism string `json:"mechanism"`
	User      string `json:"user"`

	// Password may be given literally, or as {env:VAR}, so that a config file
	// can be committed without carrying a secret.
	Password string `json:"password"`

	// PasswordFile reads the secret from a file instead, which is how secrets
	// usually arrive in Kubernetes and systemd deployments.
	PasswordFile string `json:"password_file"`
}

// TLS controls transport encryption to the brokers.
type TLS struct {
	Enabled bool   `json:"enabled"`
	CAFile  string `json:"ca_file"`
}

// HTTP controls where the server listens.
type HTTP struct {
	Address string `json:"address"`
}

// cluster mirrors one cluster's JSON exactly, so unknown keys can be rejected
// and a comma-separated broker string can be turned into a list.
type cluster struct {
	Broker   string `json:"broker"`
	ReadOnly bool   `json:"read_only"`
	TLS      *TLS   `json:"tls"`
	SASL     *SASL  `json:"sasl"`
}

// file mirrors the whole config file.
type file struct {
	HTTP      *HTTP               `json:"http"`
	OutputDir string              `json:"output_dir"`
	Clusters  map[string]*cluster `json:"clusters"`
}

// Cluster is one Kafka cluster the server can serve.
type Cluster struct {
	// Name is the key from the config file. It names the HTTP path the
	// cluster is served on and is reported by list_clusters, so it is carried
	// here rather than left as only a map key.
	Name string

	Brokers  []string
	ReadOnly bool
	TLS      *TLS
	SASL     *SASL
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

// Load reads the configuration from path.
//
// A path is required: without one there is nothing to describe the clusters,
// and guessing an address risks connecting to something the operator never
// named.
func Load(path string) (*Config, error) {
	if path == "" {
		return nil, fmt.Errorf(
			"no configuration file: set %s to the path of a JSON config file", PathEnv)
	}

	return fromFile(path)
}

// LoadDefault loads the file named by KAFKA_MCP_CONFIG.
func LoadDefault() (*Config, error) {
	return Load(os.Getenv(PathEnv))
}

func fromFile(path string) (*Config, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}

	decoder := json.NewDecoder(strings.NewReader(string(contents)))

	// An unknown key is nearly always a typo, and silently ignoring one could
	// leave an operator believing a protection is on when it is not.
	decoder.DisallowUnknownFields()

	var parsed file

	if err := decoder.Decode(&parsed); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}

	if len(parsed.Clusters) == 0 {
		return nil, fmt.Errorf(
			"config %s: at least one cluster is required, or the server serves no endpoints", path)
	}

	cfg := &Config{
		OutputDir: parsed.OutputDir,
		Clusters:  make(map[string]*Cluster, len(parsed.Clusters)),
		Path:      path,
	}

	if parsed.HTTP != nil {
		cfg.HTTP = *parsed.HTTP
	}

	if cfg.HTTP.Address == "" {
		cfg.HTTP.Address = DefaultAddress
	}

	// Exports still need somewhere to go, and the temp directory needs no
	// configuration to be usable.
	if cfg.OutputDir == "" {
		cfg.OutputDir = os.TempDir()
	}

	for name, parsedCluster := range parsed.Clusters {
		resolved, err := resolveCluster(name, parsedCluster, path)
		if err != nil {
			return nil, err
		}

		cfg.Clusters[name] = resolved
	}

	return cfg, nil
}

func resolveCluster(name string, parsed *cluster, path string) (*Cluster, error) {
	if strings.TrimSpace(name) == "" {
		return nil, fmt.Errorf("config %s: a cluster name must not be empty", path)
	}

	// The name becomes a URL path segment, so a name with a slash would make
	// the cluster unreachable rather than merely odd.
	if strings.ContainsAny(name, "/ ") {
		return nil, fmt.Errorf(
			"config %s: cluster name %q must not contain a slash or a space, because it names the endpoint path",
			path, name)
	}

	resolved := &Cluster{
		Name:     name,
		Brokers:  splitBrokers(parsed.Broker),
		ReadOnly: parsed.ReadOnly,
		TLS:      parsed.TLS,
		SASL:     parsed.SASL,
	}

	if len(resolved.Brokers) == 0 {
		return nil, fmt.Errorf("config %s: cluster %q needs a broker", path, name)
	}

	if err := resolveSASL(resolved, path); err != nil {
		return nil, err
	}

	return resolved, nil
}

// resolveSASL validates the mechanism and turns whichever password form was
// used into the actual secret.
func resolveSASL(cluster *Cluster, path string) error {
	if cluster.SASL == nil {
		return nil
	}

	mechanism := strings.ToLower(strings.TrimSpace(cluster.SASL.Mechanism))

	switch mechanism {
	case MechanismPlain, MechanismScramSHA256, MechanismScramSHA512:
		cluster.SASL.Mechanism = mechanism
	case "":
		return fmt.Errorf(
			"config %s: cluster %q sasl needs a mechanism, one of %s, %s or %s",
			path, cluster.Name, MechanismPlain, MechanismScramSHA256, MechanismScramSHA512)
	default:
		return fmt.Errorf(
			"config %s: cluster %q has unsupported sasl mechanism %q, must be one of %s, %s or %s",
			path, cluster.Name, cluster.SASL.Mechanism,
			MechanismPlain, MechanismScramSHA256, MechanismScramSHA512)
	}

	if cluster.SASL.User == "" {
		return fmt.Errorf("config %s: cluster %q sasl needs a user", path, cluster.Name)
	}

	password, err := resolvePassword(cluster.SASL, cluster.Name, path)
	if err != nil {
		return err
	}

	// Connecting anonymously when credentials were configured would silently
	// drop the identity that ACLs are enforced against.
	if password == "" {
		return fmt.Errorf(
			"config %s: cluster %q sasl user %q has no password: set password, password_file, or use {env:VAR}",
			path, cluster.Name, cluster.SASL.User)
	}

	cluster.SASL.Password = password
	cluster.SASL.PasswordFile = ""

	return nil
}

func resolvePassword(sasl *SASL, name string, path string) (string, error) {
	if sasl.PasswordFile != "" {
		if sasl.Password != "" {
			return "", fmt.Errorf(
				"config %s: cluster %q sets both password and password_file for sasl, which is ambiguous",
				path, name)
		}

		contents, err := os.ReadFile(sasl.PasswordFile)
		if err != nil {
			return "", fmt.Errorf("config %s: cluster %q read password_file: %w", path, name, err)
		}

		// Secret files almost always end in a newline, and Kafka would treat
		// it as part of the password.
		return strings.TrimSpace(string(contents)), nil
	}

	return interpolate(sasl.Password, name, path)
}

// envPlaceholder matches {env:VAR}, the form used to keep a secret out of the
// config file itself.
var envPlaceholder = regexp.MustCompile(`^\{env:([A-Za-z_][A-Za-z0-9_]*)\}$`)

func interpolate(value string, name string, path string) (string, error) {
	match := envPlaceholder.FindStringSubmatch(value)
	if match == nil {
		return value, nil
	}

	variable := match[1]

	resolved := os.Getenv(variable)
	if resolved == "" {
		return "", fmt.Errorf(
			"config %s: cluster %q references environment variable %s, which is not set",
			path, name, variable)
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

// Describe renders one cluster for reporting, with the password removed.
// A secret must never reach an MCP client.
func (c *Cluster) Describe() map[string]any {
	described := map[string]any{
		"name":      c.Name,
		"brokers":   c.Brokers,
		"read_only": c.ReadOnly,
		"tls":       c.TLS != nil && c.TLS.Enabled,
	}

	if c.SASL != nil {
		// The mechanism and principal are what an operator needs to reason
		// about ACLs. The password is never useful to a caller.
		described["authentication"] = c.SASL.Mechanism
		described["sasl_user"] = c.SASL.User
	} else {
		described["authentication"] = "none"
	}

	return described
}
