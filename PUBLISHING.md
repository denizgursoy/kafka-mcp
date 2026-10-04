# Publishing kafka-mcp

A release is a tag push. Everything below either happens on that push or is a
one-time step someone has to do by hand.

## What a tag publishes

Pushing `v1.2.3` runs `.github/workflows/tag.yml`, which:

1. **goreleaser** builds the binaries, the release archives, and the image
   `ghcr.io/denizgursoy/kafka-mcp:v1.2.3`. The image carries the label
   `io.modelcontextprotocol.server.name=io.github.denizgursoy/kafka-mcp`, which
   is how the MCP Registry checks that the image belongs to this server.
2. **`ci/mcpb/package.sh`** turns those binaries into MCP Bundles, one per
   platform: macOS on Apple Silicon, Linux amd64 and arm64, Windows amd64. It
   asks the built server for its tools, so a bundle cannot list a tool the
   binary lacks. It then writes the final `server.json`: the image tag, every
   bundle URL and its SHA-256. The bundles and `server.json` are attached to
   the GitHub release.
3. **mcp-publisher** publishes that `server.json` to the
   [official MCP Registry](https://registry.modelcontextprotocol.io). It
   authenticates with GitHub OIDC, so no secret is involved.
4. **Smithery** gets the Apple Silicon bundle, if `SMITHERY_API_KEY` is set.

Steps 3 and 4 skip prerelease tags such as `v1.2.3-rc1`, so a candidate can be
released on GitHub without being listed.

`server.json` in the repository is the template. Its `version` and image tag
are placeholders that the workflow overwrites. Edit the template to change the
description or how a client runs the image, never the generated copy.

### Why macOS ships for Apple Silicon only

A bundle can name an operating system but not a CPU, and so can a registry
entry. Each bundle must therefore carry one binary that runs everywhere its OS
does. A universal macOS binary would do that, but at 33 MB compressed it is
over Smithery's 25 MB limit, while one architecture is about 16 MB. Intel Macs
use the release archive or the image.

## Before the first release

These are one-time steps that need an account.

| Where | What to do |
| ----- | ---------- |
| MCP Registry | Nothing. The `io.github.denizgursoy/` namespace is proved by the workflow's OIDC token, because the repository is under that account. The registry pulls the image to read its label, so `ghcr.io/denizgursoy/kafka-mcp` must stay public, as it is now. |
| Smithery | Sign in at [smithery.ai](https://smithery.ai) as `denizgursoy`. Run `npx smithery auth token` and store the token as the repository secret `SMITHERY_API_KEY`. Until then, the Smithery step is skipped. |

## Catalogs that need a person

These are not on any tag. Do each once; afterwards they follow the repository
on their own, apart from Docker, which pins a commit.

| Catalog | How |
| ------- | --- |
| [Docker MCP Catalog](https://github.com/docker/mcp-registry) | Fork it, copy `ci/docker-mcp-catalog/server.yaml` to `servers/kafka-mcp/server.yaml`, and set `source.commit` to the released commit. Next to it, put the `tools.json` that `ci/mcpb/package.sh` writes to `dist/mcpb/`: the server needs a config file before it can list tools, so their build reads this file instead. Open a pull request; the Docker team reviews it. Docker builds the image itself from the root `Dockerfile`. To update, open a new pull request that bumps `source.commit`. |
| [Glama](https://glama.ai/mcp/servers) | Indexes public repositories by itself. `glama.json` names the maintainers so they can claim the listing. |
| [awesome-mcp-servers](https://github.com/punkpeye/awesome-mcp-servers) | Pull request adding one line under **Databases**, alphabetically:<br>`- [denizgursoy/kafka-mcp](https://github.com/denizgursoy/kafka-mcp) 🏎️ 🏠 🍎 🪟 🐧 - Debug Kafka: find messages by key or field, measure consumer lag and when it clears, unblock consumers, compare clusters. Read-only endpoints and previews before every write.` |
| [mcp.so](https://mcp.so), [PulseMCP](https://www.pulsemcp.com) | Submit the repository URL in their forms. PulseMCP also reads the MCP Registry, so it may list the server on its own. |
| [Cursor directory](https://cursor.com/directory) | Submit through the site. Use the stdio configuration from the README's "Connecting a client" section. |

## Checking a release before tagging

```sh
goreleaser release --snapshot --clean     # binaries and image, nothing pushed
ci/mcpb/package.sh v0.0.0-check           # bundles and server.json in dist/mcpb/
mcp-publisher validate dist/mcpb/server.json
```

The validator checks the schema only. The checks that need a release, such as
whether the image label matches and whether the bundle URLs exist, happen when
the workflow publishes.
