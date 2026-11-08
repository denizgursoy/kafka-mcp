// Package skills holds the canonical skill documents as compile-time assets.
// Installable skills are the subdirectories containing SKILL.md; this bridge
// stays outside those folders and is not part of a skill installation.
// Hosts integrate through pkg/kafkamcp, not this asset package.
package skills

import (
	"embed"
	"io/fs"
)

// Keep the source Markdown in place: ordinary Go builds always embed the current
// documents without a generated copy, a release hook or runtime disk access.
// Reference directories are recursive; the consuming tool selects only Markdown.
//
//go:embed */*.md */references
var documents embed.FS

// Documents returns the embedded, read-only filesystem rooted at skills/.
func Documents() fs.FS { return documents }
