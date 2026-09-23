package config_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	mcors "github.com/rakunlabs/ada/middleware/cors"
	"github.com/stretchr/testify/suite"

	"github.com/denizgursoy/kafka-mcp/internal/domain/config"
)

type ConfigSuite struct {
	suite.Suite
}

func TestConfigSuite(t *testing.T) {
	suite.Run(t, new(ConfigSuite))
}

// write puts a config file in a temp directory and returns its path.
func (s *ConfigSuite) write(contents string) string {
	s.T().Helper()

	path := filepath.Join(s.T().TempDir(), "kafka-mcp.json")

	s.Require().NoError(os.WriteFile(path, []byte(contents), 0o600),
		"the fixture file must be written, otherwise the case tests nothing")

	return path
}

func (s *ConfigSuite) load(path string) (*config.Config, error) {
	s.T().Helper()
	s.T().Setenv("CONFIG_FILE", path)
	return config.Load(s.T().Context())
}

func (s *ConfigSuite) TestLoadsSeveralClusters() {
	path := s.write(`{
		"http": {"address": ":9000"},
		"output_dir": "/var/tmp/exports",
		"clusters": {
			"prod":    {"broker": "kafka-1:9093,kafka-2:9093", "read_only": true},
			"preprod": {"broker": "kafka-preprod:9093"}
		}
	}`)

	loaded, err := s.load(path)

	s.Require().NoError(err, "a well-formed multi-cluster config must load")

	s.Run("every cluster is present", func() {
		s.Require().Len(loaded.Clusters, 2,
			"each configured cluster becomes its own endpoint, so losing one would silently make it unreachable")
		s.Require().Contains(loaded.Clusters, "prod",
			"a cluster must be addressable by the name it was given in the file")
		s.Require().Contains(loaded.Clusters, "preprod",
			"a cluster must be addressable by the name it was given in the file")
	})

	s.Run("brokers are split into a list", func() {
		s.Require().Equal([]string{"kafka-1:9093", "kafka-2:9093"}, loaded.Clusters["prod"].Brokers,
			"a comma-separated broker list must be split, because franz-go takes seeds individually")
	})

	s.Run("read only is per cluster", func() {
		s.Require().True(loaded.Clusters["prod"].ReadOnly,
			"read_only is the only protection on a cluster without ACLs, so it must survive parsing")
		s.Require().False(loaded.Clusters["preprod"].ReadOnly,
			"a cluster that did not ask for read_only must stay writable, or a copy destination would be refused for no reason")
	})

	s.Run("the cluster knows its own name", func() {
		s.Require().Equal("prod", loaded.Clusters["prod"].Name,
			"the name is reported by list_clusters and server_config, so it must be carried on the cluster rather than only being a map key")
	})

	s.Run("server-wide settings are read", func() {
		s.Require().Equal(":9000", loaded.HTTP.Address,
			"the listen address decides where clients connect, so a configured value must not be lost")
		s.Require().Equal("/var/tmp/exports", loaded.OutputDir,
			"exports are server-wide and must land where the operator chose")
	})
}

func (s *ConfigSuite) TestLoadsSeveralEndpointsForOneCluster() {
	loaded, err := s.load(s.write(`{
		"clusters": {
			"prod": {"broker": "kafka-1:9093,kafka-2:9093"}
		},
		"endpoints": {
			"prod-read": {
				"cluster": "prod",
				"path": "/mcp",
				"description": "Production investigation",
				"read_only": true
			},
			"prod-write": {
				"cluster": "prod",
				"path": "/mcp/rw",
				"description": "Approved production changes",
				"tools": {"copy_message": false}
			}
		}
	}`))

	s.Require().NoError(err,
		"one Kafka connection must be reusable by endpoints with different paths and permissions")
	s.Require().Len(loaded.Clusters, 1,
		"endpoint policies must not duplicate the physical Kafka cluster or its credentials")
	s.Require().Len(loaded.Endpoints, 2,
		"both policies must become independently addressable MCP endpoints")
	s.Require().Equal("prod", loaded.Endpoints["prod-read"].Cluster,
		"an endpoint must retain the cluster it binds its tools to")
	s.Require().Equal("/mcp", loaded.Endpoints["prod-read"].Path,
		"the configured path must survive in canonical absolute form")
	s.Require().Equal("Production investigation", loaded.Endpoints["prod-read"].Description,
		"the endpoint purpose must be reportable to an MCP caller")
	s.Require().True(loaded.Endpoints["prod-read"].ReadOnly,
		"the read endpoint must enforce its own policy even though another endpoint for the same cluster is writable")
	s.Require().False(loaded.Endpoints["prod-write"].ToolEnabled("copy_message"),
		"tool switches belong to the endpoint because two views of one cluster may expose different capabilities")
}

func (s *ConfigSuite) TestLegacyClustersBecomeEndpoints() {
	loaded, err := s.load(s.write(`{
		"clusters": {
			"prod": {
				"broker": "kafka:9093",
				"read_only": true,
				"tools": {"copy_message": false}
			}
		}
	}`))

	s.Require().NoError(err,
		"the existing configuration shape must keep working while deployments migrate to explicit endpoints")
	s.Require().Contains(loaded.Endpoints, "prod",
		"an old cluster entry must gain an implicit endpoint with the same stable name")
	s.Require().Equal("/mcp/prod", loaded.Endpoints["prod"].Path,
		"the compatibility endpoint must preserve the URL existing MCP clients use")
	s.Require().True(loaded.Endpoints["prod"].ReadOnly,
		"compatibility must never drop a write protection from an existing deployment")
	s.Require().False(loaded.Endpoints["prod"].ToolEnabled("copy_message"),
		"compatibility must preserve tool restrictions as well as routing")
}

func (s *ConfigSuite) TestRejectsInvalidEndpoints() {
	s.Run("unknown cluster", func() {
		_, err := s.load(s.write(`{
			"clusters": {"prod": {"broker": "kafka:9093"}},
			"endpoints": {"read": {"cluster": "missing", "path": "/mcp"}}
		}`))

		s.Require().Error(err,
			"an endpoint that is not bound to a configured cluster could only fail later on every tool call")
		s.Require().Contains(err.Error(), "missing",
			"the error must name the bad reference so the operator can repair it")
	})

	s.Run("duplicate normalized path", func() {
		_, err := s.load(s.write(`{
			"clusters": {"prod": {"broker": "kafka:9093"}},
			"endpoints": {
				"read": {"cluster": "prod", "path": "/mcp"},
				"write": {"cluster": "prod", "path": "mcp/"}
			}
		}`))

		s.Require().Error(err,
			"two endpoint names must not silently compete for the same HTTP route after path normalization")
	})

	s.Run("health route", func() {
		_, err := s.load(s.write(`{
			"clusters": {"prod": {"broker": "kafka:9093"}},
			"endpoints": {"read": {"cluster": "prod", "path": "/healthz"}}
		}`))

		s.Require().Error(err,
			"an MCP endpoint must not replace the unauthenticated liveness route")
	})
}

func (s *ConfigSuite) TestHTTPBasePath() {
	s.Run("normalizes a configured base path", func() {
		loaded, err := s.load(s.write(`{
			"http": {"base_path": "platform/kafka/"},
			"clusters": {"local": {"broker": "localhost:19092"}}
		}`))

		s.Require().NoError(err, "a relative-looking base path must be accepted for convenient configuration")
		s.Require().Equal("/platform/kafka", loaded.HTTP.BasePath,
			"the server needs one canonical form so route registration cannot produce missing or doubled slashes")
	})

	s.Run("defaults to the root", func() {
		loaded, err := s.load(s.write(`{"clusters": {"local": {"broker": "localhost:19092"}}}`))

		s.Require().NoError(err, "omitting the base path must preserve the existing root-mounted endpoints")
		s.Require().Empty(loaded.HTTP.BasePath,
			"an empty base path keeps existing deployments on /mcp and /healthz")
	})

	s.Run("treats a slash as the root", func() {
		loaded, err := s.load(s.write(`{
			"http": {"base_path": "/"},
			"clusters": {"local": {"broker": "localhost:19092"}}
		}`))

		s.Require().NoError(err, "a slash is the conventional spelling of the HTTP root")
		s.Require().Empty(loaded.HTTP.BasePath,
			"root must use the same canonical representation as an omitted base path")
	})

	s.Run("canonicalizes repeated root slashes", func() {
		loaded, err := s.load(s.write(`{
			"http": {"base_path": "////"},
			"clusters": {"local": {"broker": "localhost:19092"}}
		}`))

		s.Require().NoError(err, "repeated slashes still describe the HTTP root")
		s.Require().Empty(loaded.HTTP.BasePath,
			"root must not retain a slash that would create //mcp and //healthz routes")
	})

	s.Run("rejects query and fragment syntax", func() {
		_, err := s.load(s.write(`{
			"http": {"base_path": "/kafka?tenant=prod"},
			"clusters": {"local": {"broker": "localhost:19092"}}
		}`))

		s.Require().Error(err,
			"a base path is only a URL path; accepting query syntax would register a route different from the configured value")
	})
}

func (s *ConfigSuite) TestRequiresConfiguration() {
	_, err := s.load("")

	s.Require().Error(err,
		"the server must refuse to start without configured clusters rather than guessing a broker")
	s.Require().Contains(err.Error(), "CONFIG_FILE",
		"the error must name the variable that points at the file, so the operator knows what to set")
}

func (s *ConfigSuite) TestDiscoversConfigurationFiles() {
	s.Run("user config directory is the fallback", func() {
		workingDir := s.T().TempDir()
		userConfigDir := s.T().TempDir()
		configDir := filepath.Join(userConfigDir, "kafka-mcp")
		s.Require().NoError(os.MkdirAll(configDir, 0o700),
			"the user configuration directory must exist for the discovery case to be meaningful")
		s.Require().NoError(os.WriteFile(
			filepath.Join(configDir, "kafka-mcp.json"),
			[]byte(`{"clusters":{"user":{"broker":"user:9092"}}}`),
			0o600,
		), "the fallback configuration must be written before loading it")

		s.T().Chdir(workingDir)
		s.T().Setenv("CONFIG_FILE", "")
		s.T().Setenv("XDG_CONFIG_HOME", userConfigDir)

		loaded, err := config.Load(s.T().Context())

		s.Require().NoError(err,
			"a user-level configuration must make the server usable outside the directory that contains the file")
		s.Require().Contains(loaded.Clusters, "user",
			"the fallback must come from the kafka-mcp directory under the operating system's user config directory")
	})

	s.Run("working directory takes precedence", func() {
		workingDir := s.T().TempDir()
		userConfigDir := s.T().TempDir()
		configDir := filepath.Join(userConfigDir, "kafka-mcp")
		s.Require().NoError(os.MkdirAll(configDir, 0o700),
			"the lower-priority user configuration must exist so the case proves precedence")
		s.Require().NoError(os.WriteFile(
			filepath.Join(configDir, "kafka-mcp.json"),
			[]byte(`{"clusters":{"user":{"broker":"user:9092"}}}`),
			0o600,
		), "the lower-priority user configuration must be written before loading")
		s.Require().NoError(os.WriteFile(
			filepath.Join(workingDir, "kafka-mcp.json"),
			[]byte(`{"clusters":{"working":{"broker":"working:9092"}}}`),
			0o600,
		), "the working-directory configuration must be written before loading")

		s.T().Chdir(workingDir)
		s.T().Setenv("CONFIG_FILE", "")
		s.T().Setenv("XDG_CONFIG_HOME", userConfigDir)

		loaded, err := config.Load(s.T().Context())

		s.Require().NoError(err, "a configuration in the working directory must load")
		s.Require().Contains(loaded.Clusters, "working",
			"project-local configuration must override the user's fallback configuration")
		s.Require().NotContains(loaded.Clusters, "user",
			"chu selects the first matching file rather than merging configurations from multiple directories")
	})
}

func (s *ConfigSuite) TestRequiresAtLeastOneCluster() {
	path := s.write(`{"clusters": {}}`)

	_, err := s.load(path)

	s.Require().Error(err,
		"a server with no clusters serves no endpoints at all, which is a configuration mistake rather than a valid deployment")
}

func (s *ConfigSuite) TestRequiresABrokerPerCluster() {
	path := s.write(`{"clusters": {"prod": {"read_only": true}}}`)

	_, err := s.load(path)

	s.Require().Error(err,
		"a cluster that names no broker cannot be connected to, and guessing a default would reach somewhere the operator did not choose")
	s.Require().Contains(err.Error(), "prod",
		"the error must name the offending cluster, or an operator with several has to guess which one is wrong")
}

func (s *ConfigSuite) TestDefaultsTheListenAddress() {
	path := s.write(`{"clusters": {"local": {"broker": "localhost:19092"}}}`)

	loaded, err := s.load(path)

	s.Require().NoError(err, "omitting the http block must be allowed")
	s.Require().Equal(":8090", loaded.HTTP.Address,
		"the server must listen somewhere, and a default address means a minimal config still runs")
}

// TestCORSDefaults covers the merge chu performs over the seeded policy. A
// browser reports almost nothing when a preflight is refused, so a key
// silently lost between the file and the middleware would surface as "the
// client cannot reach the server" and nothing more.
func (s *ConfigSuite) TestCORSDefaults() {
	s.Run("an absent cors block keeps every default", func() {
		loaded, err := s.load(s.write(`{"clusters": {"local": {"broker": "localhost:19092"}}}`))

		s.Require().NoError(err, "omitting the cors block must be allowed")
		s.Require().Equal(config.DefaultCORS(), loaded.HTTP.CORS,
			"a config file that says nothing about CORS must get the whole default policy, not a zero struct")
	})

	s.Run("setting one key keeps the others", func() {
		loaded, err := s.load(s.write(`{
			"http": {"cors": {"allow_origins": ["https://allowed.example"]}},
			"clusters": {"local": {"broker": "localhost:19092"}}
		}`))

		s.Require().NoError(err, "narrowing the origins must be allowed")
		s.Require().Equal([]string{"https://allowed.example"}, loaded.HTTP.CORS.AllowOrigins,
			"the configured origin must win, or narrowing the policy does nothing")
		s.Require().Equal(config.DefaultCORS().AllowHeaders, loaded.HTTP.CORS.AllowHeaders,
			"narrowing the origins must not drop the MCP headers, or the allowed origin still cannot call the server")
		s.Require().Equal(config.DefaultCORS().AllowMethods, loaded.HTTP.CORS.AllowMethods,
			"the transport needs GET, POST and DELETE, and an unrelated key must not take them away")
		s.Require().True(loaded.HTTP.CORS.AllowPrivateNetwork,
			"an unmentioned key must keep its default, including the one that is true by default")
	})

	s.Run("private network access can be turned off", func() {
		loaded, err := s.load(s.write(`{
			"http": {"cors": {"allow_private_network": false}},
			"clusters": {"local": {"broker": "localhost:19092"}}
		}`))

		s.Require().NoError(err, "disabling private network access must be allowed")
		s.Require().False(loaded.HTTP.CORS.AllowPrivateNetwork,
			"an explicit false must be distinguishable from an absent key, or the setting cannot be turned off at all")
	})

	s.Run("the defaults answer the preflight an MCP client sends", func() {
		handler := mcors.Middleware(mcors.WithConfig(config.DefaultCORS()))(
			http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusOK)
			}))

		request := httptest.NewRequestWithContext(
			s.T().Context(), http.MethodOptions, "/mcp/local", nil)
		request.Header.Set("Origin", "https://mcp-client.example")
		request.Header.Set("Access-Control-Request-Method", http.MethodPost)
		request.Header.Set("Access-Control-Request-Headers",
			"content-type,mcp-session-id,mcp-protocol-version")
		request.Header.Set("Access-Control-Request-Private-Network", "true")

		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)

		s.Require().Equal("*", recorder.Header().Get("Access-Control-Allow-Origin"),
			"without an allow-origin the browser discards the response, so the client never sees the server at all")
		s.Require().Contains(recorder.Header().Get("Access-Control-Allow-Headers"), "mcp-session-id",
			"every request after initialize carries the session id, so refusing it limits the client to one call")
		s.Require().Contains(recorder.Header().Get("Access-Control-Allow-Headers"), "mcp-protocol-version",
			"the SDK sends the negotiated protocol version on later requests, so refusing it breaks them")
		s.Require().Contains(recorder.Header().Get("Access-Control-Allow-Methods"), http.MethodDelete,
			"a client ends its session with DELETE, so refusing it leaks sessions on the server")
		s.Require().Equal("true", recorder.Header().Get("Access-Control-Allow-Private-Network"),
			"a public page reaching a server on a private address is blocked by Chrome without this, which is the common case for a local broker")
	})

	s.Run("the session id is readable by the page", func() {
		handler := mcors.Middleware(mcors.WithConfig(config.DefaultCORS()))(
			http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusOK)
			}))

		request := httptest.NewRequestWithContext(
			s.T().Context(), http.MethodPost, "/mcp/local", nil)
		request.Header.Set("Origin", "https://mcp-client.example")

		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)

		s.Require().Equal(http.StatusOK, recorder.Code,
			"a non-preflight request must reach the handler, not be answered by the CORS middleware")
		s.Require().Contains(recorder.Header().Get("Access-Control-Expose-Headers"), "Mcp-Session-Id",
			"an unexposed session id is invisible to JavaScript, so the client cannot continue the session it just opened")
	})
}

func (s *ConfigSuite) TestOutputDirectoryFallsBackToTheTempDirectory() {
	path := s.write(`{"clusters": {"local": {"broker": "localhost:19092"}}}`)

	loaded, err := s.load(path)

	s.Require().NoError(err, "omitting the output directory must be allowed")
	s.Require().Equal(os.TempDir(), loaded.OutputDir,
		"exports still need somewhere to go, and the temp directory is a safe default that needs no configuration")
}

func (s *ConfigSuite) TestResolvesPasswordPerCluster() {
	s.T().Setenv("PROD_PASSWORD", "prod-secret")

	path := s.write(`{
		"clusters": {
			"prod": {
				"broker": "kafka:9093",
				"sasl": {"mechanism": "scram-sha-256", "user": "ali", "password": "{env:PROD_PASSWORD}"}
			},
			"local": {"broker": "localhost:19092"}
		}
	}`)

	loaded, err := s.load(path)

	s.Require().NoError(err, "interpolating a password from the environment must succeed")
	s.Require().Len(loaded.Clusters["prod"].SASL, 1,
		"the legacy single sasl block must resolve to a list of exactly one mechanism")
	s.Require().Equal("prod-secret", loaded.Clusters["prod"].SASL[0].Password,
		"the placeholder must be replaced with the real secret, which is what keeps it out of the file")
	s.Require().Empty(loaded.Clusters["local"].SASL,
		"a cluster without authentication must stay unauthenticated, not inherit another cluster's identity")
}

func (s *ConfigSuite) TestErrorsWhenTheInterpolatedVariableIsMissing() {
	s.T().Setenv("PROD_PASSWORD", "")

	path := s.write(`{
		"clusters": {
			"prod": {
				"broker": "kafka:9093",
				"sasl": {"mechanism": "plain", "user": "ali", "password": "{env:PROD_PASSWORD}"}
			}
		}
	}`)

	_, err := s.load(path)

	s.Require().Error(err,
		"an unset variable must fail loudly: connecting with an empty password would look like a credentials problem at the broker instead of a configuration mistake")
}

func (s *ConfigSuite) TestReadsPasswordFromAFile() {
	secret := filepath.Join(s.T().TempDir(), "secret")

	s.Require().NoError(os.WriteFile(secret, []byte("file-secret\n"), 0o600),
		"the secret fixture must be written")

	path := s.write(`{
		"clusters": {
			"prod": {
				"broker": "kafka:9093",
				"sasl": {"mechanism": "plain", "user": "ali", "password_file": "` + secret + `"}
			}
		}
	}`)

	loaded, err := s.load(path)

	s.Require().NoError(err, "reading a password from a file must succeed")
	s.Require().Len(loaded.Clusters["prod"].SASL, 1,
		"the legacy single sasl block must resolve to a list of exactly one mechanism")
	s.Require().Equal("file-secret", loaded.Clusters["prod"].SASL[0].Password,
		"the trailing newline must be trimmed, because mounted secret files almost always carry one and Kafka would reject the password")
}

func (s *ConfigSuite) TestIgnoresUnknownKeys() {
	path := s.write(`{"extra": true, "clusters": {"prod": {"broker": "kafka:9093", "unused": true}}}`)

	loaded, err := s.load(path)

	s.Require().NoError(err, "unknown fields must be ignored by the standard chu loader")
	s.Require().Equal([]string{"kafka:9093"}, loaded.Clusters["prod"].Brokers,
		"known settings must still load when extra fields are present")
}

func (s *ConfigSuite) TestRejectsMalformedJSON() {
	path := s.write(`{"clusters": `)

	_, err := s.load(path)

	s.Require().Error(err,
		"an unparseable file must fail rather than fall back to defaults that point somewhere unintended")
}

func (s *ConfigSuite) TestErrorsWhenTheFileIsMissing() {
	_, err := s.load(filepath.Join(s.T().TempDir(), "absent.json"))

	s.Require().Error(err,
		"a config path that does not exist is a mistake worth reporting, not a reason to start with defaults")
}

func (s *ConfigSuite) TestRejectsAnUnknownSASLMechanism() {
	path := s.write(`{
		"clusters": {
			"prod": {"broker": "kafka:9093", "sasl": {"mechanism": "kerberos", "user": "ali", "password": "x"}}
		}
	}`)

	_, err := s.load(path)

	s.Require().Error(err,
		"an unsupported mechanism must be refused at startup rather than failing obscurely on the first connection")
}

func (s *ConfigSuite) TestRejectsSASLWithoutCredentials() {
	path := s.write(`{
		"clusters": {"prod": {"broker": "kafka:9093", "sasl": {"mechanism": "plain"}}}
	}`)

	_, err := s.load(path)

	s.Require().Error(err,
		"a mechanism without credentials must fail: connecting anonymously instead would silently drop the identity that ACLs are enforced against")
}

func (s *ConfigSuite) TestSortsClusterNames() {
	path := s.write(`{
		"clusters": {
			"preprod": {"broker": "b:9092"},
			"local":   {"broker": "c:9092"},
			"prod":    {"broker": "a:9092"}
		}
	}`)

	loaded, err := s.load(path)

	s.Require().NoError(err, "the config must load")
	s.Require().Equal([]string{"local", "preprod", "prod"}, loaded.ClusterNames(),
		"cluster names must be sorted, because Go map order is random and list_clusters would otherwise return a different order every call")
}

func (s *ConfigSuite) TestPerClusterToolSwitches() {
	path := s.write(`{
		"clusters": {
			"prod": {
				"broker": "a:9092",
				"tools": {"create_topic": false, "commit_offset": false, "list_topics": true}
			},
			"local": {"broker": "b:9092"}
		}
	}`)

	loaded, err := s.load(path)

	s.Require().NoError(err, "a config that switches tools off must load")

	s.Run("a tool set to false is disabled", func() {
		s.Require().False(loaded.Clusters["prod"].ToolEnabled("create_topic"),
			"the whole purpose of the setting is that the named tool is not exposed")
	})

	s.Run("a tool the map does not mention stays enabled", func() {
		s.Require().True(loaded.Clusters["prod"].ToolEnabled("consumer_lag"),
			"the map lists exceptions, so a deployment states what it withholds rather than having to relist every tool and silently lose whatever is added later")
	})

	s.Run("a tool set to true stays enabled", func() {
		s.Require().True(loaded.Clusters["prod"].ToolEnabled("list_topics"),
			"an explicit true must read the same as an absent key, or the map would behave as an allow-list on the clusters that use it")
	})

	s.Run("a cluster with no tool settings enables everything", func() {
		s.Require().True(loaded.Clusters["local"].ToolEnabled("create_topic"),
			"a cluster that says nothing about tools must keep them all, or adding the setting to one cluster would change another")
	})

	s.Run("the disabled tools are reported in a stable order", func() {
		s.Require().Equal([]string{"commit_offset", "create_topic"}, loaded.Clusters["prod"].DisabledTools(),
			"Go map order is random, and a startup log that lists the same tools in a different order every run cannot be compared between restarts")
	})
}

func (s *ConfigSuite) TestLoadsLocalYAML() {
	loaded, err := s.load(filepath.Join("..", "..", "..", "kafka-mcp.local.yaml"))
	s.Require().NoError(err, "the committed local YAML must work with CONFIG_FILE")
	s.Require().Equal("127.0.0.1:8090", loaded.HTTP.Address,
		"the committed local configuration must bind the loopback address on the port that avoids the Console")
	s.Require().Equal([]string{"localhost:19092"}, loaded.Clusters["local"].Brokers,
		"the local config must reach the compose broker's published port")
}

func (s *ConfigSuite) TestEnvironmentOverridesFile() {
	s.T().Setenv("KAFKA_MCP_HTTP_ADDRESS", ":9001")
	s.T().Setenv("KAFKA_MCP_HTTP_BASE_PATH", "/gateway/kafka/")
	path := s.write(`{"http":{"address":":8090"},"clusters":{"local":{"broker":"localhost:19092"}}}`)
	loaded, err := s.load(path)
	s.Require().NoError(err, "chu environment overrides must work after loading the file")
	s.Require().Equal(":9001", loaded.HTTP.Address, "the prefixed environment setting must override the file")
	s.Require().Equal("/gateway/kafka", loaded.HTTP.BasePath,
		"the base path must be configurable from the environment for container deployments that do not mount a config file")
}

func (s *ConfigSuite) TestRejectsNullCluster() {
	_, err := s.load(s.write(`{"clusters":{"local":null}}`))
	s.Require().Error(err, "a null cluster must return an error rather than panic during startup")
}

func (s *ConfigSuite) TestSecurityConfiguration() {
	s.Run("ordered mechanisms and TLS", func() {
		s.T().Setenv("SECURITY_PASSWORD", "secret-from-env")
		loaded, err := s.load(s.write(`{"clusters":{"prod":{"broker":"kafka:9093","security":{
			"tls":{"enabled":true,"ca_file":"ca.pem","cert_file":"client.pem","key_file":"client.key"},
			"sasl":[
				{"scram":{"enabled":true,"algorithm":"SCRAM-SHA-512","user":"alice","pass":"{env:SECURITY_PASSWORD}","zid":"delegate","is_token":true}},
				{"plain":{"enabled":true,"user":"bob","pass":"other-secret","zid":"proxy"}}
			]}}}}`))
		s.Require().NoError(err, "wkafka-style security must load with independently configured mechanisms")
		cfg := loaded.Clusters["prod"]
		s.Require().Len(cfg.SASL, 2, "all enabled mechanisms must survive parsing for broker negotiation")
		s.Require().Equal(config.MechanismScramSHA512, cfg.SASL[0].Mechanism, "preference order must survive normalization")
		s.Require().Equal("secret-from-env", cfg.SASL[0].Password, "environment secrets must resolve inside security.sasl")
		s.Require().True(cfg.SASL[0].IsToken, "delegation tokens require the SCRAM token attribute")
		s.Require().Equal("delegate", cfg.SASL[0].Zid, "authorization identity must survive parsing")
		s.Require().Equal("proxy", cfg.SASL[1].Zid, "PLAIN must also preserve authorization identity")
		s.Require().Equal("client.pem", cfg.TLS.CertFile, "mTLS needs the client certificate as well as a CA")
		s.Require().Equal("client.key", cfg.TLS.KeyFile, "mTLS needs the key paired with the certificate")
		s.Require().NotContains(cfg.Describe(), "password", "configuration reporting must never expose credentials")
	})
	s.Run("secret file", func() {
		secret := s.write("mounted-secret\n")
		loaded, err := s.load(s.write(`{"clusters":{"prod":{"broker":"kafka:9093","security":{"sasl":[{"plain":{"enabled":true,"user":"alice","password_file":"` + secret + `"}}]}}}}`))
		s.Require().NoError(err, "mounted secrets must remain usable in the new layout")
		s.Require().Len(loaded.Clusters["prod"].SASL, 1, "one enabled entry must resolve to one mechanism")
		s.Require().Equal("mounted-secret", loaded.Clusters["prod"].SASL[0].Password, "mounted secret newlines must not become part of the password")
	})
	s.Run("disabled mechanisms", func() {
		loaded, err := s.load(s.write(`{"clusters":{"local":{"broker":"localhost:9092","security":{"sasl":[{"scram":{"algorithm":"invalid","pass":"{env:UNUSED_SECRET}"}}]}}}}`))
		s.Require().NoError(err, "disabled entries must not require credentials or resolve unused secrets")
		s.Require().Empty(loaded.Clusters["local"].SASL, "no enabled entries means a plaintext unauthenticated connection")
	})
	s.Run("mixed layouts", func() {
		_, err := s.load(s.write(`{"clusters":{"prod":{"broker":"kafka:9093","tls":{"enabled":true},"security":{}}}}`))
		s.Require().ErrorContains(err, "legacy", "mixed layouts must fail instead of silently ignoring TLS")
	})
	s.Run("ambiguous entry", func() {
		_, err := s.load(s.write(`{"clusters":{"prod":{"broker":"kafka:9093","security":{"sasl":[{"plain":{"enabled":true},"scram":{"enabled":true}}]}}}}`))
		s.Require().ErrorContains(err, "only one", "one list entry must not silently choose between two enabled identities")
	})
	s.Run("invalid algorithm", func() {
		_, err := s.load(s.write(`{"clusters":{"prod":{"broker":"kafka:9093","security":{"sasl":[{"scram":{"enabled":true,"algorithm":"PLAIN","user":"a","pass":"b"}}]}}}}`))
		s.Require().ErrorContains(err, "scram requires", "SCRAM cannot silently downgrade to PLAIN")
	})
	s.Run("missing credentials", func() {
		_, err := s.load(s.write(`{"clusters":{"prod":{"broker":"kafka:9093","security":{"sasl":[{"plain":{"enabled":true,"user":"alice"}}]}}}}`))
		s.Require().ErrorContains(err, "no password", "enabled authentication must not silently become anonymous")
	})
	s.Run("conflicting secret sources", func() {
		_, err := s.load(s.write(`{"clusters":{"prod":{"broker":"kafka:9093","security":{"sasl":[{"plain":{"enabled":true,"user":"alice","pass":"secret","password_file":"secret.txt"}}]}}}}`))
		s.Require().ErrorContains(err, "both password and password_file", "ambiguous secret sources must be rejected before connecting")
	})
	s.Run("incomplete client certificate", func() {
		_, err := s.load(s.write(`{"clusters":{"prod":{"broker":"kafka:9093","security":{"tls":{"enabled":true,"cert_file":"client.pem"}}}}}`))
		s.Require().ErrorContains(err, "supplied together", "a missing private key must not silently disable mTLS")
	})
	s.Run("duplicate mechanisms", func() {
		_, err := s.load(s.write(`{"clusters":{"prod":{"broker":"kafka:9093","security":{"sasl":[{"plain":{"enabled":true,"user":"a","pass":"b"}},{"plain":{"enabled":true,"user":"c","pass":"d"}}]}}}}`))
		s.Require().ErrorContains(err, "duplicate", "mechanism fallback is not a way to retry different passwords")
	})
}

func (s *ConfigSuite) TestOAuthConfiguration() {
	s.Run("client credentials and duration", func() {
		s.T().Setenv("OAUTH_SECRET", "client-secret-value")
		loaded, err := s.load(s.write(`{"clusters":{"prod":{"broker":"kafka:9093","security":{"sasl":[{"oauth":{"enabled":true,"token_url":"https://idp.example/token","client_id":"kafka-mcp","client_secret":"{env:OAUTH_SECRET}","scopes":["kafka"],"timeout":"3s","zid":"delegate","extensions":{"tenant":"test"}}}]}}}}`))
		s.Require().NoError(err, "OAuth client credentials must load using wkafka's configuration names")
		s.Require().Len(loaded.Clusters["prod"].SASL, 1, "one enabled OAuth entry must resolve to one mechanism")
		auth := loaded.Clusters["prod"].SASL[0]
		s.Require().Equal(config.MechanismOAuth, auth.Mechanism, "OAuth must select OAUTHBEARER, not anonymous authentication")
		s.Require().Equal("client-secret-value", auth.OAuth.ClientSecret, "client secrets must support environment substitution")
		s.Require().Equal(3*time.Second, auth.OAuth.Timeout, "duration strings must bound token endpoint requests")
		s.Require().Equal([]string{"kafka"}, auth.OAuth.Scopes, "requested scopes must reach the token endpoint")
		data, err := json.Marshal(loaded.Clusters["prod"].Describe())
		s.Require().NoError(err, "safe configuration reporting must remain serializable")
		s.Require().NotContains(string(data), "client-secret-value", "OAuth secrets must never reach MCP clients")
		s.Require().Contains(string(data), "kafka-mcp", "the client id is useful non-secret configuration information")
	})
	s.Run("static token", func() {
		s.T().Setenv("OAUTH_TOKEN", "static-token-secret")
		loaded, err := s.load(s.write(`{"clusters":{"prod":{"broker":"kafka:9093","security":{"sasl":[{"oauth":{"enabled":true,"token":"{env:OAUTH_TOKEN}"}}]}}}}`))
		s.Require().NoError(err, "static tokens must not require client credentials")
		s.Require().Len(loaded.Clusters["prod"].SASL, 1, "a static token must still resolve to one mechanism")
		s.Require().Equal("static-token-secret", loaded.Clusters["prod"].SASL[0].OAuth.Token, "token placeholders must be resolved before authentication")
		data, err := json.Marshal(loaded.Clusters["prod"].Describe())
		s.Require().NoError(err, "static-token configuration must be reportable")
		s.Require().NotContains(string(data), "static-token-secret", "static bearer tokens must never be reported")
	})
	s.Run("missing token source", func() {
		_, err := s.load(s.write(`{"clusters":{"prod":{"broker":"kafka:9093","security":{"sasl":[{"oauth":{"enabled":true}}]}}}}`))
		s.Require().ErrorContains(err, "exactly one", "enabled OAuth must have a usable token source")
	})
	s.Run("mixed sources", func() {
		_, err := s.load(s.write(`{"clusters":{"prod":{"broker":"kafka:9093","security":{"sasl":[{"oauth":{"enabled":true,"token":"token","client_id":"client"}}]}}}}`))
		s.Require().ErrorContains(err, "exactly one", "static tokens must not silently override client credentials")
	})
	s.Run("missing client secret", func() {
		_, err := s.load(s.write(`{"clusters":{"prod":{"broker":"kafka:9093","security":{"sasl":[{"oauth":{"enabled":true,"token_url":"https://idp/token","client_id":"client"}}]}}}}`))
		s.Require().ErrorContains(err, "required", "incomplete client credentials must fail at startup")
	})
	s.Run("invalid endpoint", func() {
		_, err := s.load(s.write(`{"clusters":{"prod":{"broker":"kafka:9093","security":{"sasl":[{"oauth":{"enabled":true,"token_url":"file:///token","client_id":"client","client_secret":"secret"}}]}}}}`))
		s.Require().ErrorContains(err, "HTTP or HTTPS", "token endpoints must be absolute HTTP URLs")
	})
	s.Run("negative timeout", func() {
		_, err := s.load(s.write(`{"clusters":{"prod":{"broker":"kafka:9093","security":{"sasl":[{"oauth":{"enabled":true,"token":"token","timeout":"-1s"}}]}}}}`))
		s.Require().ErrorContains(err, "negative", "negative token timeouts are invalid configuration")
	})
	s.Run("OAuth and SCRAM in one entry", func() {
		_, err := s.load(s.write(`{"clusters":{"prod":{"broker":"kafka:9093","security":{"sasl":[{"oauth":{"enabled":true,"token":"token"},"scram":{"enabled":true}}]}}}}`))
		s.Require().ErrorContains(err, "only one", "OAuth must participate in the per-entry exclusivity check")
	})
}
