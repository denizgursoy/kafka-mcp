// Package outputfile confines the files tools read and write to the one
// directory the server is configured with.
//
// It lives in internal/domain because several tools share it: search_messages
// and transform_messages write messages to a file, and produce_message reads
// one back. What they must agree on is that a caller names a file and never a
// path, so an MCP client cannot make the server read or write wherever it
// likes.
package outputfile

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// Path validates a caller-supplied file name and returns the absolute path it
// names inside dir. field is the parameter the name came from, so a refusal
// names what the caller actually set.
func Path(dir string, field string, name string) (string, error) {
	if strings.TrimSpace(name) == "" {
		return "", fmt.Errorf("%s is empty", field)
	}

	// Anything that could escape the directory is rejected outright rather
	// than cleaned, so a rejected name is obvious to the caller.
	if strings.ContainsAny(name, `/\`) || strings.Contains(name, "..") {
		return "", fmt.Errorf(
			"%s %q must be a file name, not a path: the server chooses the directory", field, name)
	}

	if dir == "" {
		dir = os.TempDir()
	}

	return filepath.Join(dir, name), nil
}

// Writer writes values to a new file, one JSON document per line, so a tool
// that produces a great many messages never has to hold them all in memory or
// return them to the caller.
type Writer struct {
	file    *os.File
	encoder *json.Encoder
	written int
}

// Create opens a new file named name in dir. An existing name is refused.
func Create(dir string, field string, name string) (*Writer, error) {
	path, err := Path(dir, field, name)
	if err != nil {
		return nil, err
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create output directory %s: %w", filepath.Dir(path), err)
	}

	// O_EXCL refuses an existing name, which also refuses a planted symlink:
	// with O_CREAT it fails on any existing entry, link or not, instead of
	// following it. An earlier export is never silently replaced.
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if errors.Is(err, fs.ErrExist) {
		return nil, fmt.Errorf("%s %q already exists in %s: choose a new name", field, name, filepath.Dir(path))
	}

	if err != nil {
		return nil, fmt.Errorf("create %s %s: %w", field, path, err)
	}

	return &Writer{file: file, encoder: json.NewEncoder(file)}, nil
}

// Write appends one value as a line of JSON.
func (w *Writer) Write(value any) error {
	if err := w.encoder.Encode(value); err != nil {
		return fmt.Errorf("write to %s: %w", w.file.Name(), err)
	}

	w.written++

	return nil
}

// Path is the absolute path of the file being written.
func (w *Writer) Path() string {
	return w.file.Name()
}

// Written is how many values have been written.
func (w *Writer) Written() int {
	return w.written
}

// Close closes the file. Closing twice is harmless.
func (w *Writer) Close() error {
	err := w.file.Close()
	if errors.Is(err, os.ErrClosed) {
		return nil
	}

	return err
}

// Open opens an existing file named name in dir for reading.
//
// A symlink is refused, so a link planted in the directory cannot make the
// server read a file outside it.
func Open(dir string, field string, name string) (*os.File, error) {
	path, err := Path(dir, field, name)
	if err != nil {
		return nil, err
	}

	info, err := os.Lstat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("%s %q does not exist in %s", field, name, filepath.Dir(path))
	}

	if err != nil {
		return nil, fmt.Errorf("read %s %s: %w", field, path, err)
	}

	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%s %q is not a regular file", field, name)
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s %s: %w", field, path, err)
	}

	return file, nil
}
