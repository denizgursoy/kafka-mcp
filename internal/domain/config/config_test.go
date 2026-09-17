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

func (s *ConfigSuite) TestLoadsAConfigFile() {
	path := s.write(`{
		"environment": "staging",
		"broker": "kafka-1:9093,kafka-2:9093",
		"read_only": true,
		"output_dir": "/var/tmp/exports"
	}`)

	loaded, err := config.Load(path)

	s.Require().NoError(err, "a well-formed config file must load")

	s.Run("brokers are split into a list", func() {
		s.Require().Equal([]string{"kafka-1:9093", "kafka-2:9093"}, loaded.Brokers,
			"a comma-separated broker list must be split, because franz-go takes seeds individually")
	})

	s.Run("the environment label is kept", func() {
		s.Require().Equal("staging", loaded.Environment,
			"the label is informational, but server_config reports it so an operator can confirm which cluster this is")
	})

	s.Run("read only is honoured", func() {
		s.Require().True(loaded.ReadOnly,
			"read_only is the only protection on a cluster without ACLs, so it must never be lost in parsing")
	})

	s.Run("the output directory is kept", func() {
		s.Require().Equal("/var/tmp/exports", loaded.OutputDir,
			"exports must land where the operator chose, not in a default the caller cannot predict")
	})
}

func (s *ConfigSuite) TestRequiresAConfigFile() {
	_, err := config.Load("")

	s.Require().Error(err,
		"the server must refuse to start without a config file: guessing a broker address risks connecting to something the operator never named")
	s.Require().Contains(err.Error(), config.PathEnv,
		"the error must name the variable that points at the file, so the operator knows what to set")
}

func (s *ConfigSuite) TestIgnoresEnvironmentVariablesEntirely() {
	s.T().Setenv("KAFKA_BROKER", "env-broker:9092")
	s.T().Setenv("KAFKA_MCP_OUTPUT_DIR", "/env/exports")

	path := s.write(`{"broker": "file-broker:9093", "output_dir": "/file/exports"}`)

	loaded, err := config.Load(path)

	s.Require().NoError(err, "a config file alongside stale environment variables must load")

	s.Run("the broker comes from the file", func() {
		s.Require().Equal([]string{"file-broker:9093"}, loaded.Brokers,
			"the file is the only source of configuration, so a leftover environment variable must not redirect the server elsewhere")
	})

	s.Run("the output directory comes from the file", func() {
		s.Require().Equal("/file/exports", loaded.OutputDir,
			"every setting comes from the file, never a mixture of file and environment")
	})
}

func (s *ConfigSuite) TestRequiresABroker() {
	path := s.write(`{"environment": "local"}`)

	_, err := config.Load(path)

	s.Require().Error(err,
		"a config file that names no broker cannot be used, and guessing a default would connect somewhere the operator did not choose")
}

func (s *ConfigSuite) TestOutputDirectoryFallsBackToTheTempDirectory() {
	path := s.write(`{"broker": "localhost:19092"}`)

	loaded, err := config.Load(path)

	s.Require().NoError(err, "omitting the output directory must be allowed")
	s.Require().Equal(os.TempDir(), loaded.OutputDir,
		"exports still need somewhere to go, and the temp directory is a safe default that needs no configuration")
}

func (s *ConfigSuite) TestResolvesPasswordFromTheEnvironment() {
	s.T().Setenv("KAFKA_PASSWORD", "s3cret")

	path := s.write(`{
		"broker": "kafka:9093",
		"sasl": {"mechanism": "scram-sha-256", "user": "ali", "password": "{env:KAFKA_PASSWORD}"}
	}`)

	loaded, err := config.Load(path)

	s.Require().NoError(err, "interpolating a password from the environment must succeed")
	s.Require().Equal("s3cret", loaded.SASL.Password,
		"the placeholder must be replaced with the real secret, which is what keeps it out of the file")
	s.Require().Equal("ali", loaded.SASL.User,
		"the principal must survive parsing, because Kafka ACLs are enforced against it")
}

func (s *ConfigSuite) TestErrorsWhenTheInterpolatedVariableIsMissing() {
	s.T().Setenv("KAFKA_PASSWORD", "")

	path := s.write(`{
		"broker": "kafka:9093",
		"sasl": {"mechanism": "plain", "user": "ali", "password": "{env:KAFKA_PASSWORD}"}
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
		"broker": "kafka:9093",
		"sasl": {"mechanism": "plain", "user": "ali", "password_file": "` + secret + `"}
	}`)

	loaded, err := config.Load(path)

	s.Require().NoError(err, "reading a password from a file must succeed")
	s.Require().Equal("file-secret", loaded.SASL.Password,
		"the trailing newline must be trimmed, because mounted secret files almost always carry one and Kafka would reject the password")
}

func (s *ConfigSuite) TestRejectsUnknownKeys() {
	path := s.write(`{"broker": "kafka:9093", "readonly": true}`)

	_, err := config.Load(path)

	s.Require().Error(err,
		"a misspelled key must fail: silently ignoring \"readonly\" would leave an operator believing writes are blocked when they are not")
}

func (s *ConfigSuite) TestRejectsMalformedJSON() {
	path := s.write(`{"broker": `)

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
		"broker": "kafka:9093",
		"sasl": {"mechanism": "kerberos", "user": "ali", "password": "x"}
	}`)

	_, err := config.Load(path)

	s.Require().Error(err,
		"an unsupported mechanism must be refused at startup rather than failing obscurely on the first connection")
}

func (s *ConfigSuite) TestRejectsSASLWithoutCredentials() {
	path := s.write(`{"broker": "kafka:9093", "sasl": {"mechanism": "plain"}}`)

	_, err := config.Load(path)

	s.Require().Error(err,
		"a mechanism without credentials must fail: connecting anonymously instead would silently drop the identity that ACLs are enforced against")
}

func (s *ConfigSuite) TestAcceptsAConfigWithoutSASL() {
	path := s.write(`{"broker": "localhost:19092", "environment": "local"}`)

	loaded, err := config.Load(path)

	s.Require().NoError(err,
		"a cluster without authentication is normal in development and must not require SASL settings")
	s.Require().Nil(loaded.SASL,
		"no SASL configuration must mean no SASL, not an empty mechanism that changes how the client connects")
}
