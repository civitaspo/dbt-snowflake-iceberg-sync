# Securefix setup

This repository uses [`csm-actions/securefix-action`](https://github.com/csm-actions/securefix-action) and related CSM actions so pull request workflows can request signed commits, approvals, and releases without holding strong credentials.

The Securefix server repository name is configured with `SECUREFIX_SERVER_REPOSITORY` (repository name only, not `owner/repo`).

## GitHub Apps

Install both apps on this repository and on the configured Securefix server repository:

- Client app: `issues: write` (creates request labels on the server repo)
- Server app: `contents: write`, `actions: read`, `pull_requests: write`, and `workflows: write`

## Variables and secrets (this repository)

- Variable `SECUREFIX_CLIENT_APP_ID`
- Secret `SECUREFIX_CLIENT_PRIVATE_KEY`
- Variable `SECUREFIX_SERVER_REPOSITORY` (server repository name only; `securefix-action` expects this format)
- Variable `SECUREFIX_APPROVE_ACTORS` (JSON array of GitHub logins that may auto-request approval on `pull_request_target`)
- Variable `SECUREFIX_APPROVE_COMMENT_USER` (login allowed to trigger approval with a `/approve` comment)
- Variable `SECUREFIX_ALLOWED_COMMITTERS` (newline-separated committers accepted by the client approve action; keep in sync with the server approve workflow)

Strong secrets (GPG keys, machine-user PAT, server app private key) live only in the configured Securefix server repository. Workflows fail clearly when required client configuration is missing.

## Flows

### Lint autofix (Securefix)

The `Lint` workflow runs fixers (`pinact`, `disable-checkout-persist-credentials`). When a pull request needs fixes, it requests a Securefix commit. The server workflow accepts client workflows named `Lint` and `Release PR`.

### Auto-approval

The `Approve Request` workflow asks the server to approve pull requests authored by logins listed in `SECUREFIX_APPROVE_ACTORS` (and on `/approve` comments from `SECUREFIX_APPROVE_COMMENT_USER`). Both the client and server actions validate that all commits are signed and that committers are in `SECUREFIX_ALLOWED_COMMITTERS` (action defaults are only Renovate/Dependabot). When client validation passes, it creates a label on the Securefix server repository; the server then approves with the machine-user PAT.

## Dependency updates

- **Version updates:** [Renovate](https://docs.renovatebot.com/) (`renovate.json5`) owns GitHub Actions and mise tooling. Non-major updates automerge when checks pass.
- **Dependabot version updates:** Not configured. There is no `.github/dependabot.yml`.
- **Dependabot security updates:** May remain enabled via the GitHub repository security settings. Those PRs are eligible for Approve Request when `dependabot[bot]` is listed in `SECUREFIX_APPROVE_ACTORS` and `SECUREFIX_ALLOWED_COMMITTERS`.

### Release

1. The `Release PR` workflow asks Securefix to open or update `release/next` with changelog and version metadata.
2. After that PR is squash-merged, the `Release Tag` workflow creates an annotated tag and requests a server-side release.
3. The server publishes the GitHub Release for the tag.

See [releasing.md](releasing.md).
