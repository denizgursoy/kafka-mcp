// Package config loads the server's own configuration: which cluster to talk
// to, how to authenticate, and what the server is allowed to do.
//
// Configuration comes from a JSON file named by KAFKA_MCP_CONFIG, and from
// nowhere else. One file describes a whole deployment, so there is no way for
// a stale environment variable to point the server at a cluster nobody
// intended.
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"
)

// PathEnv names the config file to load. It is the only environment variable
// the server reads, because it needs some way to find the file in the first
// place.
const PathEnv = "KAFKA_MCP_CONFIG"

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

// file mirrors the JSON config exactly. It is separate from Config so that
// unknown keys can be rejected and a comma-separated broker string can be
// turned into a list.
type file struct {
	Environment string `json:"environment"`
	Broker      string `json:"broker"`
	ReadOnly    bool   `json:"read_only"`
	OutputDir   string `json:"output_dir"`
	TLS         *TLS   `json:"tls"`
	SASL        *SASL  `json:"sasl"`
}

// Config is the effective configuration the server runs with.
type Config struct {
	// Environment is a free-form label such as "production". It is
	// informational: it changes no behaviour and is reported by server_config
	// so an operator can confirm which cluster a session is talking to.
	Environment string

	Brokers   []string
	ReadOnly  bool
	OutputDir string
	TLS       *TLS
	SASL      *SASL

	// Path is the file this came from, empty when the environment was used.
	Path string
}

// Load reads the configuration from path.
//
// A path is required: without one there is nothing to describe the cluster,
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

	cfg := &Config{
		Environment: parsed.Environment,
		Brokers:     splitBrokers(parsed.Broker),
		ReadOnly:    parsed.ReadOnly,
		OutputDir:   parsed.OutputDir,
		TLS:         parsed.TLS,
		SASL:        parsed.SASL,
		Path:        path,
	}

	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("config %s: broker is required", path)
	}

	if err := resolveSASL(cfg, path); err != nil {
		return nil, err
	}

	// Exports still need somewhere to go, and the temp directory needs no
	// configuration to be usable.
	if cfg.OutputDir == "" {
		cfg.OutputDir = os.TempDir()
	}

	return cfg, nil
}

// resolveSASL validates the mechanism and turns whichever password form was
// used into the actual secret.
func resolveSASL(cfg *Config, path string) error {
	if cfg.SASL == nil {
		return nil
	}

	mechanism := strings.ToLower(strings.TrimSpace(cfg.SASL.Mechanism))

	switch mechanism {
	case MechanismPlain, MechanismScramSHA256, MechanismScramSHA512:
		cfg.SASL.Mechanism = mechanism
	case "":
		return fmt.Errorf(
			"config %s: sasl needs a mechanism, one of %s, %s or %s",
			path, MechanismPlain, MechanismScramSHA256, MechanismScramSHA512)
	default:
		return fmt.Errorf(
			"config %s: unsupported sasl mechanism %q, must be one of %s, %s or %s",
			path, cfg.SASL.Mechanism,
			MechanismPlain, MechanismScramSHA256, MechanismScramSHA512)
	}

	if cfg.SASL.User == "" {
		return fmt.Errorf("config %s: sasl needs a user", path)
	}

	password, err := resolvePassword(cfg.SASL, path)
	if err != nil {
		return err
	}

	// Connecting anonymously when credentials were configured would silently
	// drop the identity that ACLs are enforced against.
	if password == "" {
		return fmt.Errorf(
			"config %s: sasl user %q has no password: set password, password_file, or use {env:VAR}",
			path, cfg.SASL.User)
	}

	cfg.SASL.Password = password
	cfg.SASL.PasswordFile = ""

	return nil
}

func resolvePassword(sasl *SASL, path string) (string, error) {
	if sasl.PasswordFile != "" {
		if sasl.Password != "" {
			return "", fmt.Errorf(
				"config %s: set either password or password_file for sasl, not both", path)
		}

		contents, err := os.ReadFile(sasl.PasswordFile)
		if err != nil {
			return "", fmt.Errorf("config %s: read password_file: %w", path, err)
		}

		// Secret files almost always end in a newline, and Kafka would treat
		// it as part of the password.
		return strings.TrimSpace(string(contents)), nil
	}

	return interpolate(sasl.Password, path)
}

// envPlaceholder matches {env:VAR}, the form used to keep a secret out of the
// config file itself.
var envPlaceholder = regexp.MustCompile(`^\{env:([A-Za-z_][A-Za-z0-9_]*)\}$`)

func interpolate(value string, path string) (string, error) {
	match := envPlaceholder.FindStringSubmatch(value)
	if match == nil {
		return value, nil
	}

	name := match[1]

	resolved := os.Getenv(name)
	if resolved == "" {
		return "", fmt.Errorf(
			"config %s: environment variable %s is referenced by the config but is not set",
			path, name)
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

// Describe renders the configuration for reporting, with the password
// removed. A secret must never reach an MCP client.
func (c *Config) Describe() map[string]any {
	described := map[string]any{
		"brokers":    c.Brokers,
		"read_only":  c.ReadOnly,
		"output_dir": c.OutputDir,
		"tls":        c.TLS != nil && c.TLS.Enabled,
	}

	if c.Environment != "" {
		described["environment"] = c.Environment
	}

	if c.Path != "" {
		described["config_file"] = c.Path
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
