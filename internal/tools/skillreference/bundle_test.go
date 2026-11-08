package skillreference

import (
	"io/fs"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/denizgursoy/kafka-mcp/skills"
)

// BundleSuite intentionally needs no broker: it compares release data with its
// source, so missing embed patterns also fail the fast -short development check.
type BundleSuite struct{ suite.Suite }

func TestBundleSuite(t *testing.T) { suite.Run(t, new(BundleSuite)) }

func (s *BundleSuite) TestBundleExactlyMatchesCanonicalMarkdown() {
	bundled, err := catalog()
	s.Require().NoError(err, "embedded Markdown must be readable without generating any Go source")
	root := filepath.Join("..", "..", "..", "skills")
	skills, err := filepath.Glob(filepath.Join(root, "*", "SKILL.md"))
	s.Require().NoError(err, "canonical skill roots must be discoverable")
	s.Require().NotEmpty(skills, "an empty source inventory must not validate an accidentally empty bundle")
	expected := make(map[string]map[string]string, len(skills))
	for _, skill := range skills {
		dir := filepath.Dir(skill)
		docs := make(map[string]string)
		err := filepath.WalkDir(dir, func(filename string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".md") {
				return nil
			}
			relative, err := filepath.Rel(dir, filename)
			if err != nil {
				return err
			}
			content, err := os.ReadFile(filename)
			if err != nil {
				return err
			}
			docs[filepath.ToSlash(relative)] = string(content)
			return nil
		})
		s.Require().NoError(err, "source documents must be readable so the freshness check cannot silently miss files")
		expected[filepath.Base(dir)] = docs
	}
	s.Require().Equal(expected, bundled,
		"go:embed must include every canonical Markdown document, including added and removed files")
}

func (s *BundleSuite) TestMarkdownLinksResolveToBundledDocuments() {
	bundled, err := catalog()
	s.Require().NoError(err, "the embedded catalog must be readable before resolving scenario links")
	links := regexp.MustCompile(`\]\(([^)]+)\)`)
	for skill, docs := range bundled {
		for file, content := range docs {
			for _, match := range links.FindAllStringSubmatch(content, -1) {
				link, err := url.Parse(match[1])
				s.Require().NoError(err, "the link in %s/%s must be readable by a remote agent", skill, file)
				if link.IsAbs() || link.Host != "" || !strings.HasSuffix(link.Path, ".md") {
					continue
				}
				target := path.Join(path.Dir(file), link.Path)
				s.Require().Contains(docs, target,
					"link %s in %s/%s must resolve to another bundled guide, not a file only present in a checkout", match[1], skill, file)
			}
		}
	}
}

func (s *BundleSuite) TestEmbeddingBridgeStaysOutsideInstallableSkills() {
	_, err := fs.Stat(skills.Documents(), "embed.go")
	s.Require().ErrorIs(err, fs.ErrNotExist, "the asset bridge must not be part of the embedded document surface")
	roots, err := filepath.Glob(filepath.Join("..", "..", "..", "skills", "*", "SKILL.md"))
	s.Require().NoError(err, "installable skills are still discovered by their SKILL.md")
	s.Require().NotEmpty(roots, "embedding must not remove the externally installable skill folders")
	for _, root := range roots {
		goFiles, err := filepath.Glob(filepath.Join(filepath.Dir(root), "*.go"))
		s.Require().NoError(err, "the installable skill folder must be inspectable")
		s.Require().Empty(goFiles, "copying %s must not install Go bridge files into an agent's skill directory", filepath.Dir(root))
	}
}
