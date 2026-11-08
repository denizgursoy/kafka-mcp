package skillreference

import (
	"fmt"
	"io/fs"
	"path"
	"strings"

	"github.com/denizgursoy/kafka-mcp/skills"
)

// catalog builds exact-name lookup tables from the embedded Markdown, never
// paths on disk. The embedding package carries data only; selection belongs to
// this tool, so returning its filesystem cannot widen the tool's read surface.
func catalog() (map[string]map[string]string, error) {
	documents := skills.Documents()
	roots, err := fs.Glob(documents, "*/SKILL.md")
	if err != nil {
		return nil, fmt.Errorf("list embedded skills: %w", err)
	}
	result := make(map[string]map[string]string, len(roots))
	for _, root := range roots {
		dir := path.Dir(root)
		docs := make(map[string]string)
		err := fs.WalkDir(documents, dir, func(file string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".md") {
				return nil
			}
			content, err := fs.ReadFile(documents, file)
			if err != nil {
				return err
			}
			docs[strings.TrimPrefix(file, dir+"/")] = string(content)
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("read embedded skill %s: %w", dir, err)
		}
		result[dir] = docs
	}
	return result, nil
}
