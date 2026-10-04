#!/usr/bin/env bash
# Builds the release's MCP Bundles and the server.json published to the MCP
# Registry, from binaries goreleaser has already built into dist/.
#
#   ci/mcpb/package.sh <tag>        e.g. ci/mcpb/package.sh v0.3.0
#
# Writes to dist/mcpb/:
#   kafka-mcp_<version>_<os>_<arch>.mcpb   one bundle per platform
#   server.json                            server.json with the OCI image and every bundle
#   tools.json                             the tool list in the Docker MCP Catalog's format
#
# Needs jq and npx (for the official mcpb CLI).
set -euo pipefail

tag=${1:?usage: package.sh <tag>}
version=${tag#v}
repo=denizgursoy/kafka-mcp
root=$(cd "$(dirname "$0")/../.." && pwd)
out=$root/dist/mcpb
mcpb="npx -y @anthropic-ai/mcpb@2.1.2"

# goos goarch mcpb-platform. macOS ships for Apple Silicon only: neither a
# bundle nor the MCP Registry can say which CPU a file is for, and a universal
# binary would exceed Smithery's 25 MB bundle limit.
platforms=(
  "darwin arm64 darwin"
  "linux amd64 linux"
  "linux arm64 linux"
  "windows amd64 win32"
)

binary() {
  jq -er --arg os "$1" --arg arch "$2" \
    '.[] | select(.type == "Binary" and .goos == $os and .goarch == $arch) | .path' \
    "$root/dist/artifacts.json"
}

rm -rf "$out"
mkdir -p "$out"

# The tool list comes from the server itself, so the bundle can never describe
# tools the binary does not have. An unreachable broker is fine: tools are
# registered whether or not the cluster answers.
host=$(binary "$(go env GOOS)" "$(go env GOARCH)")
probe=$(mktemp -d)
trap 'rm -rf "$probe"' EXIT
printf 'clusters:\n  probe:\n    brokers: 127.0.0.1:1\n' > "$probe/config.yaml"
{
  printf '%s\n' \
    '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"package","version":"1"}}}' \
    '{"jsonrpc":"2.0","method":"notifications/initialized"}' \
    '{"jsonrpc":"2.0","id":2,"method":"tools/list"}'
  sleep 3
} | CONFIG_FILE="$probe/config.yaml" LOG_LEVEL=error "$root/$host" 2>/dev/null \
  | jq -c 'select(.id == 2) | .result.tools' > "$probe/tools.json"

if [ "$(jq length "$probe/tools.json")" -eq 0 ]; then
  echo "package.sh: the server listed no tools" >&2
  exit 1
fi

jq '[.[] | {name, description: (.description | ltrimstr("\n") | split("\n\n")[0] | gsub("\n"; " "))}]' \
  "$probe/tools.json" > "$probe/manifest-tools.json"

jq '[.[] | {
  name,
  description: (.description | ltrimstr("\n") | split("\n\n")[0] | gsub("\n"; " ")),
  arguments: [(.inputSchema.properties // {}) | to_entries[] | {
    name: .key,
    type: (.value.type // "object" | if type == "array" then map(select(. != "null"))[0] else . end),
    desc: (.value.description // "")
  }]
}]' "$probe/tools.json" > "$out/tools.json"

packages='[]'
for entry in "${platforms[@]}"; do
  read -r goos goarch platform <<< "$entry"
  exe=kafka-mcp
  [ "$goos" = windows ] && exe=kafka-mcp.exe

  dir=$probe/$goos-$goarch
  mkdir -p "$dir/server"
  cp "$root/$(binary "$goos" "$goarch")" "$dir/server/$exe"
  chmod 0755 "$dir/server/$exe"

  jq --arg version "$version" --arg exe "server/$exe" --arg platform "$platform" \
    --slurpfile tools "$probe/manifest-tools.json" '
    .version = $version
    | .server.entry_point = $exe
    | .server.mcp_config.command = "${__dirname}/" + $exe
    | .compatibility.platforms = [$platform]
    | .tools = $tools[0]
  ' "$root/ci/mcpb/manifest.json" > "$dir/manifest.json"

  file=kafka-mcp_${version}_${goos}_${goarch}.mcpb
  $mcpb pack "$dir" "$out/$file" > /dev/null
  sha=$(sha256sum "$out/$file" | cut -d' ' -f1)

  packages=$(jq --arg url "https://github.com/$repo/releases/download/$tag/$file" --arg sha "$sha" \
    --arg version "$version" --slurpfile manifest "$dir/manifest.json" '
    . + [{
      registryType: "mcpb",
      identifier: $url,
      version: $version,
      fileSha256: $sha,
      transport: {type: "stdio"},
      packageArguments: [{
        type: "named",
        name: "--endpoint",
        description: $manifest[0].user_config.endpoint.description
      }],
      environmentVariables: [{
        name: "CONFIG_FILE",
        description: $manifest[0].user_config.config_file.description,
        format: "filepath",
        isRequired: true
      }]
    }]' <<< "$packages")

  echo "$file $sha"
done

jq --arg version "$version" --arg tag "$tag" --argjson mcpb "$packages" '
  .version = $version
  | .packages |= map(if .registryType == "oci" then .identifier = (.identifier | sub(":[^:]+$"; ":" + $tag)) else . end)
  | .packages += $mcpb
' "$root/server.json" > "$out/server.json"
