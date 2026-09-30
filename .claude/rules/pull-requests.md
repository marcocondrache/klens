# Pull requests

## Before opening

Re-read the full diff. Delete comments that narrate what the code does and keep only a comment that explains a non-obvious why. Strip filler from any prose the diff adds.

Rebase into small, ordered commits. Each commit lands on its own and the order tells the story. Amend when a fix belongs in the commit just made, and add a new commit when the fix is separable.

## Size and stacks

Prefer several narrow PRs to one large PR. A stack is a chain of base branches. The root PR targets `main`. Each child branch rebases onto its parent's exact tip and its PR targets the parent branch, via `gh pr create --base <parent-branch>` or `gh pr edit <pr> --base <parent-branch>`. Branch from `main` only for independent work.

## Title

Use `type(scope): Subject`. The squash commit takes this title and git-cliff puts it in the changelog, so write it for a reader of the release notes.

- Type is one of `feat`, `fix`, `perf`, `refactor`, `docs`, `test`, `ci`, or `chore`.
- Scope is the changed area, such as `config`, `scan`, `kafka`, `server`, or `web`. Omit it when the change spans the crate.
- Subject starts with a capital letter, uses the imperative, and has no trailing period.
- Name the behavior that changes, or the real symbol that carries it. `fix(server): Log the request path instead of the full URI`, not `fix(server): Fix logging issue`.

## Body

Write the body as plain prose, the way you would explain the change to a colleague. The squash commit body is the PR body.

- Open with one to three sentences on why the change exists and what changes for the user or the next maintainer.
- Add a sentence on a decision only when a reviewer would otherwise ask about it.
- State a breaking change in its own sentence that starts with `Breaking:` and names every config key, flag, or API it affects.
- Do not use headings, bold, tables, or checklists.
- Do not restate the diff, list files or SHAs, restate the title, or paste verification logs.

Write in short declarative sentences. Keep articles. Use one word for each action and a plain verb over an `-ing` form. Do not use em dashes, and do not join clauses with a colon.

## Opening

Use `gh` to create, edit, view, and merge. Open every PR ready for review, never as a draft. Run `gh pr view <number>` before you state a PR's status.

Opening a PR does not start a watch on it. Post the URL as `https://github.com/<owner>/<repo>/pull/<number>` and continue with the remaining work. Check CI and review threads only when asked.
