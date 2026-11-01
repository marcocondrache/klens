#!/usr/bin/env bash
set -uo pipefail

path=$(jq -r '.tool_input.file_path // empty')
[[ -f "$path" ]] || exit 0

root="${CLAUDE_PROJECT_DIR:-$(git rev-parse --show-toplevel)}"

case "$path" in
  *.rs)
    rustfmt --edition 2024 "$path"
    ;;
  *.toml)
    command -v taplo >/dev/null && taplo fmt "$path"
    ;;
  "$root"/web/*.ts | "$root"/web/*.tsx | "$root"/web/*.css | "$root"/web/*.json)
    vp="$root/web/node_modules/.bin/vp"
    [[ -x "$vp" ]] && (cd "$root/web" && "$vp" fmt "$path" >/dev/null)
    ;;
esac

exit 0
