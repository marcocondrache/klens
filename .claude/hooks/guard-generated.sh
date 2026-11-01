#!/usr/bin/env bash
set -euo pipefail

path=$(jq -r '.tool_input.file_path // empty')

case "$path" in
  */web/src/api/types.gen.ts | web/src/api/types.gen.ts)
    echo "web/src/api/types.gen.ts is generated. Change the Rust types and run \`mise run types\`." >&2
    exit 2
    ;;
esac
