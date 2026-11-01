#!/usr/bin/env bash
set -uo pipefail

root="${CLAUDE_PROJECT_DIR:-$(git rev-parse --show-toplevel)}"
cd "$root" || exit 0

# Key the result on the working tree so an unchanged tree is checked once.
# A tree that failed is not checked again either, which ends the loop when
# the agent stops without a fix and reports the failure instead.
state="$(git rev-parse --git-dir)/claude-checked"
index=$(mktemp)
trap 'rm -f "$index"' EXIT
cp "$(git rev-parse --git-dir)/index" "$index"
tree=$(GIT_INDEX_FILE="$index" git add -A >/dev/null 2>&1 && GIT_INDEX_FILE="$index" git write-tree)
[[ -f "$state" && "$(cat "$state")" == "$tree" ]] && exit 0

base=$(git merge-base HEAD origin/main 2>/dev/null || git rev-parse HEAD)
changed=$(git diff --name-only "$base" "$tree")
[[ -n "$changed" ]] || exit 0

checks=()
if grep -qE '\.rs$|(^|/)Cargo\.(toml|lock)$' <<<"$changed"; then
  checks+=("cargo fmt --all --check")
  checks+=("mkdir -p static && cargo clippy --workspace --all-targets --all-features -- -D warnings")
  if cargo nextest --version >/dev/null 2>&1; then
    checks+=("cargo nextest run --workspace --exclude e2e")
  else
    checks+=("cargo test --workspace --exclude e2e")
  fi
fi
if grep -qE '\.toml$' <<<"$changed" && command -v taplo >/dev/null; then
  checks+=("taplo fmt --check")
fi
if grep -qE '^web/' <<<"$changed"; then
  if command -v mise >/dev/null; then
    checks+=("mise run web:check")
  else
    checks+=("cd web && vp check")
  fi
fi

for check in "${checks[@]}"; do
  if ! output=$(PATH="$root/web/node_modules/.bin:$PATH" bash -c "$check" 2>&1); then
    {
      echo "\`$check\` failed. Fix it before you finish, or report the failure if you cannot."
      tail -n 60 <<<"$output"
    } >&2
    echo "$tree" >"$state"
    exit 2
  fi
done

echo "$tree" >"$state"
