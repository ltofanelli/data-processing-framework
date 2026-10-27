# Contributing

## Branches

- `main` is always releasable. It only receives changes through pull requests, merged with squash merge (one commit per pull request).
- Work happens in short-lived branches created from `main`:

  | Prefix | Use |
  |---|---|
  | `feat/<short-description>` | New feature or parameter |
  | `fix/<short-description>` | Bug fix |
  | `docs/<short-description>` | Documentation only |
  | `chore/<short-description>` | Tooling, build and maintenance |

- Delete the branch after the merge.

## Commits and pull requests

- Everything is written in English: code, comments, documentation, commit messages and pull requests.
- The pull request title becomes the commit message on `main`. Write it in the imperative and keep it short (for example, "Add file discovery with file_compare").
- Cite the rule IDs from [docs/spec.md](docs/spec.md) when a change implements or changes a rule.
- A behavior change updates `docs/spec.md` in the same pull request, and `docs/architecture.md` when it affects diagrams or examples.

## Versioning

Versions follow semantic versioning, as tags on `main`: `vMAJOR.MINOR.PATCH`.

| Part | When it changes |
|---|---|
| MAJOR | An existing configuration stops working or changes behavior |
| MINOR | A new parameter or feature, compatible with existing configurations |
| PATCH | A fix that keeps the specified behavior |

- The spec's major version matches the framework's: spec v1 corresponds to releases `v1.x.y`.
- Pre-release tags (`v1.0.0-alpha.1`, `v1.0.0-beta.1`) can be used to test unreleased builds in real pipelines.
- `v1.0.0` is tagged when every acceptance scenario in the spec (`AC-01` to `AC-30`) passes.
- Documentation-only changes do not need a new tag.
- A tag is never moved or deleted after it is pushed.

## Release branches

Release branches exist only when an older major version needs a fix after `main` has moved on:

1. Create `release/<major>.x` from the last tag of that major version (for example, `release/1.x` from `v1.4.2`).
2. Make the fix through a pull request into the release branch.
3. Tag the new patch on the release branch (`v1.4.3`).
4. Bring the fix to `main` as well, if it still applies.
