package transfer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// saveNow opens kind's work and runs it into a file in the hub's directory.
func (h *Hub) saveNow(ctx context.Context, kind string, args any, filename string) (Ticket, error) {
	work, err := h.localWork(ctx, kind, args)
	if err != nil {
		return Ticket{}, err
	}

	download, ok := work.(Download)
	if !ok {
		return Ticket{}, fmt.Errorf("transfer: %s is not a download", kind)
	}

	path, result, err := h.save(ctx, filename, download)
	if err != nil {
		return Ticket{}, err
	}

	return Ticket{
		Method: MethodLocal,
		Path:   path,
		Result: result,
		Note:   "The file was saved on this machine at path. Nothing is stored anywhere else.",
	}, nil
}

// readNow opens kind's work and runs it on a file in the hub's directory.
func (h *Hub) readNow(ctx context.Context, kind string, args any, filename string) (Ticket, error) {
	work, err := h.localWork(ctx, kind, args)
	if err != nil {
		return Ticket{}, err
	}

	upload, ok := work.(Upload)
	if !ok {
		return Ticket{}, fmt.Errorf("transfer: %s is not an upload", kind)
	}

	path, err := h.local(filename)
	if err != nil {
		return Ticket{}, err
	}

	info, err := os.Stat(path)
	if err != nil {
		return Ticket{}, fmt.Errorf("read %s: %w", path, err)
	}
	if !info.Mode().IsRegular() {
		return Ticket{}, fmt.Errorf("%s is not a regular file", path)
	}

	file, err := os.Open(path)
	if err != nil {
		return Ticket{}, fmt.Errorf("read %s: %w", path, err)
	}
	defer file.Close()

	result, err := upload(ctx, file)
	if err != nil {
		return Ticket{}, err
	}

	return Ticket{
		Method: MethodLocal,
		Path:   path,
		Result: result,
		Note:   "The file was read on this machine from path.",
	}, nil
}

// localWork opens the work the way a link would, so both paths run the same
// code: args go through JSON exactly as they would inside a link.
func (h *Hub) localWork(ctx context.Context, kind string, args any) (any, error) {
	raw, err := json.Marshal(args)
	if err != nil {
		return nil, fmt.Errorf("transfer: encode the request: %w", err)
	}

	return h.work(ctx, kind, raw)
}

// save runs a download into a file. It writes to a partial file first and
// links it into place only once the download succeeded, so a file that is
// there is complete, and it never replaces an existing file.
func (h *Hub) save(ctx context.Context, filename string, download Download) (string, any, error) {
	path, err := h.local(filename)
	if err != nil {
		return "", nil, err
	}

	if _, err := os.Lstat(path); err == nil {
		return "", nil, fmt.Errorf("%s already exists: choose a new filename, an existing file is never replaced", path)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return "", nil, fmt.Errorf("check %s: %w", path, err)
	}

	partial, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*.part")
	if err != nil {
		return "", nil, fmt.Errorf("create %s: %w", path, err)
	}
	defer os.Remove(partial.Name())
	// CreateTemp makes the file private; a download is the user's own file
	// and gets the permissions any file they save would, before the umask.
	_ = partial.Chmod(0o644)

	result, err := download(ctx, partial)
	if closeErr := partial.Close(); err == nil && closeErr != nil {
		err = fmt.Errorf("write %s: %w", path, closeErr)
	}
	if err != nil {
		return "", nil, err
	}

	// Link refuses an existing name, so a file that appeared meanwhile is
	// not replaced either.
	if err := os.Link(partial.Name(), path); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return "", nil, fmt.Errorf("%s appeared while it was being written: choose a new filename", path)
		}
		// Some file systems have no hard links; fall back to a rename that
		// was checked just before.
		if _, statErr := os.Lstat(path); statErr == nil {
			return "", nil, fmt.Errorf("%s appeared while it was being written: choose a new filename", path)
		}
		if err := os.Rename(partial.Name(), path); err != nil {
			return "", nil, fmt.Errorf("save %s: %w", path, err)
		}
	}

	return path, result, nil
}

// CheckFilename refuses a name the hub would not save under. A link's name
// only goes into the command the agent runs, in the user's chosen directory,
// so a path there would save somewhere else; local allows one, because over
// stdio the caller names a file on their own machine.
func CheckFilename(name string, local bool) error {
	name = strings.TrimSpace(name)
	switch {
	case name == "":
		return fmt.Errorf("a filename is required")
	case strings.ContainsRune(name, 0):
		return fmt.Errorf("%q is not a file name", name)
	case local:
		return nil
	case strings.ContainsAny(name, `/\`) || name == "." || name == "..":
		return fmt.Errorf("%q must be a file name, not a path", name)
	}

	return nil
}

// local resolves filename in the hub's directory. A relative name, including
// one with subdirectories, is taken from there; an absolute one is used as
// is. Over stdio the server is the caller's own process, so this is the
// caller's file system, not a shared server's.
func (h *Hub) local(filename string) (string, error) {
	filename = strings.TrimSpace(filename)
	if filename == "" {
		return "", fmt.Errorf("a filename is required")
	}

	if filepath.IsAbs(filename) {
		return filepath.Clean(filename), nil
	}

	dir, err := filepath.Abs(h.dir)
	if err != nil {
		return "", fmt.Errorf("resolve the working directory: %w", err)
	}

	return filepath.Join(dir, filename), nil
}
