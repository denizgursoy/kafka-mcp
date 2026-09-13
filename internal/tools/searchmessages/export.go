package searchmessages

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/denizgursoy/kafka-mcp/internal/domain/records"
)

// OutputDirEnv names the environment variable that chooses where exported
// results are written.
const OutputDirEnv = "KAFKA_MCP_OUTPUT_DIR"

// outputDir returns the directory exports are written to. Writing is confined
// to this one directory: the caller supplies a file name, never a path, so an
// MCP client cannot make the server write wherever it likes.
func outputDir() string {
	if dir := os.Getenv(OutputDirEnv); dir != "" {
		return dir
	}

	return os.TempDir()
}

// exportPath validates a caller-supplied file name and returns the absolute
// path to write it to.
func exportPath(name string) (string, error) {
	if strings.TrimSpace(name) == "" {
		return "", fmt.Errorf("output_file is empty")
	}

	// Anything that could escape the export directory is rejected outright
	// rather than cleaned, so a rejected name is obvious to the caller.
	if strings.ContainsAny(name, `/\`) || strings.Contains(name, "..") {
		return "", fmt.Errorf(
			"output_file %q must be a file name, not a path: the server chooses the directory", name)
	}

	dir := outputDir()

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("create output directory %s: %w", dir, err)
	}

	return filepath.Join(dir, name), nil
}

// exporter writes matches to a file as they are found, one JSON message per
// line, so a search that matches a great many messages never has to hold them
// all in memory or return them to the caller.
type exporter struct {
	file    *os.File
	encoder *json.Encoder
	written int
}

func newExporter(name string) (*exporter, error) {
	path, err := exportPath(name)
	if err != nil {
		return nil, err
	}

	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create output file %s: %w", path, err)
	}

	return &exporter{file: file, encoder: json.NewEncoder(file)}, nil
}

func (e *exporter) write(message records.Message) error {
	if err := e.encoder.Encode(message); err != nil {
		return fmt.Errorf("write to %s: %w", e.file.Name(), err)
	}

	e.written++

	return nil
}

func (e *exporter) path() string {
	return e.file.Name()
}

func (e *exporter) Close() error {
	return e.file.Close()
}
