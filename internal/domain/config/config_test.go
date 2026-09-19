package config_test

import (
	"os"
	"path/filepath"
	"testing"

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

func (s *ConfigSuite) TestLoadsSeveralClusters() {
	path := s.write(`{
		"http": {"address": ":9000"},
		"output_dir": "/var/tmp/exports",
		"clusters": {
			"prod":    {"broker": "kafka-1:9093,kafka-2:9093", "read_only": true},
			"preprod": {"broker": "kafka-preprod:9093"}
		}
	}`)

	loaded, err := config.Load(path)

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

func (s *ConfigSuite) TestRequiresAConfigFile() {
	_, err := config.Load("")

	s.Require().Error(err,
		"the server must refuse to start without a config file: guessing a broker address risks connecting to something the operator never named")
	s.Require().Contains(err.Error(), config.PathEnv,
		"the error must name the variable that points at the file, so the operator knows what to set")
}

func (s *ConfigSuite) TestRequiresAtLeastOneCluster() {
	path := s.write(`{"clusters": {}}`)

	_, err := config.Load(path)

	s.Require().Error(err,
		"a server with no clusters serves no endpoints at all, which is a configuration mistake rather than a valid deployment")
}

func (s *ConfigSuite) TestRequiresABrokerPerCluster() {
	path := s.write(`{"clusters": {"prod": {"read_only": true}}}`)

	_, err := config.Load(path)

	s.Require().Error(err,
		"a cluster that names no broker cannot be connected to, and guessing a default would reach somewhere the operator did not choose")
	s.Require().Contains(err.Error(), "prod",
		"the error must name the offending cluster, or an operator with several has to guess which one is wrong")
}

func (s *ConfigSuite) TestDefaultsTheListenAddress() {
	path := s.write(`{"clusters": {"local": {"broker": "localhost:19092"}}}`)

	loaded, err := config.Load(path)

	s.Require().NoError(err, "omitting the http block must be allowed")
	s.Require().NotEmpty(loaded.HTTP.Address,
		"the server must listen somewhere, and a default address means a minimal config still runs")
}

func (s *ConfigSuite) TestOutputDirectoryFallsBackToTheTempDirectory() {
	path := s.write(`{"clusters": {"local": {"broker": "localhost:19092"}}}`)

	loaded, err := config.Load(path)

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

	loaded, err := config.Load(path)

	s.Require().NoError(err, "interpolating a password from the environment must succeed")
	s.Require().Equal("prod-secret", loaded.Clusters["prod"].SASL.Password,
		"the placeholder must be replaced with the real secret, which is what keeps it out of the file")
	s.Require().Nil(loaded.Clusters["local"].SASL,
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

	_, err := config.Load(path)

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

	loaded, err := config.Load(path)

	s.Require().NoError(err, "reading a password from a file must succeed")
	s.Require().Equal("file-secret", loaded.Clusters["prod"].SASL.Password,
		"the trailing newline must be trimmed, because mounted secret files almost always carry one and Kafka would reject the password")
}

func (s *ConfigSuite) TestRejectsUnknownKeys() {
	path := s.write(`{"clusters": {"prod": {"broker": "kafka:9093", "readonly": true}}}`)

	_, err := config.Load(path)

	s.Require().Error(err,
		"a misspelled key must fail: silently ignoring \"readonly\" would leave an operator believing writes are blocked when they are not")
}

func (s *ConfigSuite) TestRejectsMalformedJSON() {
	path := s.write(`{"clusters": `)

	_, err := config.Load(path)

	s.Require().Error(err,
		"an unparseable file must fail rather than fall back to defaults that point somewhere unintended")
}

func (s *ConfigSuite) TestErrorsWhenTheFileIsMissing() {
	_, err := config.Load(filepath.Join(s.T().TempDir(), "absent.json"))

	s.Require().Error(err,
		"a config path that does not exist is a mistake worth reporting, not a reason to start with defaults")
}

func (s *ConfigSuite) TestRejectsAnUnknownSASLMechanism() {
	path := s.write(`{
		"clusters": {
			"prod": {"broker": "kafka:9093", "sasl": {"mechanism": "kerberos", "user": "ali", "password": "x"}}
		}
	}`)

	_, err := config.Load(path)

	s.Require().Error(err,
		"an unsupported mechanism must be refused at startup rather than failing obscurely on the first connection")
}

func (s *ConfigSuite) TestRejectsSASLWithoutCredentials() {
	path := s.write(`{
		"clusters": {"prod": {"broker": "kafka:9093", "sasl": {"mechanism": "plain"}}}
	}`)

	_, err := config.Load(path)

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

	loaded, err := config.Load(path)

	s.Require().NoError(err, "the config must load")
	s.Require().Equal([]string{"local", "preprod", "prod"}, loaded.ClusterNames(),
		"cluster names must be sorted, because Go map order is random and list_clusters would otherwise return a different order every call")
}
