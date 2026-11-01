#!/usr/bin/env bash
# The pull-request skill holds the rules a script cannot check.
set -euo pipefail

input=$(cat)
tool=$(jq -r '.tool_name' <<<"$input")

if [[ "$tool" == "Bash" ]]; then
  command=$(jq -r '.tool_input.command // empty' <<<"$input")
  [[ "$command" =~ gh[[:space:]]+pr[[:space:]]+create ]] || exit 0

  draft=false
  [[ "$command" =~ --draft ]] && draft=true
  title=""
  if [[ "$command" =~ (--title|-t)[[:space:]]+\"([^\"]*)\" ]] || [[ "$command" =~ (--title|-t)[[:space:]]+\'([^\']*)\' ]]; then
    title=${BASH_REMATCH[2]}
  fi
else
  draft=$(jq -r '.tool_input.draft // false' <<<"$input")
  title=$(jq -r '.tool_input.title // empty' <<<"$input")
fi

if [[ "$draft" == "true" ]]; then
  echo "Open the pull request ready for review. Drop the draft flag." >&2
  exit 2
fi

pattern='^(feat|fix|perf|refactor|docs|test|ci|chore)(\([a-z0-9-]+\))?: [^A-Z].*[^.]$'
if [[ -n "$title" && ! "$title" =~ $pattern ]]; then
  echo "Title \"$title\" does not match \`type(scope): subject\`. Use a type from feat, fix, perf, refactor, docs, test, ci, or chore, start the subject in lower case, and drop the trailing period." >&2
  exit 2
fi
