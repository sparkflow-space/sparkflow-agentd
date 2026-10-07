# CLAUDE.md — sparkflow-agentd

The host daemon for [[CLI Agent Sessions]]: it owns CLI-agent processes (`claude`,
`opencode-go`, `qwen`, `kimi`, …) as **tmux sessions** for ONE person, and since AGENTD-003
(v0.2.0) it **dials out**: `init` enrols the device as one of that person's hosts, and `run`
holds one outbound channel (`agent.v1.HostChannel`) to `agent-sessions`, over which every
operation arrives. It opens no port.

**This repository is different from every other one in the estate in two ways. Both matter
before you touch anything.**

1. **It lives on GitHub** — `github.com/sparkflow-space/sparkflow-agentd`, public, so
   `go install …@latest` works. This is the repository's HOME, not a mirror: branches,
   pull requests and CI (GitHub Actions, `.github/workflows/test.yml`) are all here. It is
   the one repository in the estate that is not on GitLab, by the owner's decision.
   _(It started on GitLab with GitHub planned as a release mirror, like `sparkflow-sync`.
   That was a deviation from the owner's instruction, reverted on 2026-09-27.)_
   Two consequences: **nothing secret may live in source, defaults or history** — the
   repository is public — and **a published `path@version` is cached by
   `proxy.golang.org` forever**: a bad release is fixed by the next PATCH, never by
   moving a tag.
2. **It runs other people's code.** Everything below follows from that.

## The rules that are not negotiable

- **A caller passes a `kind`, never a command line.** Kinds are resolved against the
  daemon's own allow-list in the config file. This is the last place able to enforce it:
  the service above may be wrong or compromised.
- **The ENVIRONMENT is an allow-list for the same reason**, defaulting to `SPARKFLOW_`.
  An unfiltered environment hands the `kind` rule away: `LD_PRELOAD` runs the caller's
  code inside the allow-listed binary *before* its `main`, and `BASH_ENV`,
  `PYTHONSTARTUP`, `NODE_OPTIONS`, `PERL5OPT` and `GIT_SSH_COMMAND` are the same hole in
  other spellings. Measured on this host, not theorised.
- **A working folder must resolve under the owner's HOME — after symlinks — AND exist.**
  The folder is the one the person chose for the project (it arrives in `Start`); the check
  is separator-aware (`/home/u-evil` is not inside `/home/u`), symlinks are resolved first
  (a link inside home pointing at `/etc` is `/etc`), and existence is checked apart because
  tmux answers `-c /does/not/exist` with exit 0 and runs the agent in its OWN directory.
  A git repository gets a fresh worktree `<repo>/.claude/worktrees/<label>`.
- **A session belongs to the person who started it.** `List` returns only the caller's
  own; every other RPC answers `NOT_FOUND` for a session it does not own, so an id is not
  an oracle. Ownership lives on the tmux session as `@sfagent_owner`, because tmux is what
  outlives the daemon.
- **A host is single-owner.** Every operation names its owner; `handler/channel` refuses and
  audits anything naming someone other than the person who ran `init`. agent-sessions
  already routes only that person's work here — this is the check that turns a misroute
  upstream into a refusal instead of code running as the wrong person.
- **No listening port; the bearer travels only over TLS** (`http://` is accepted for loopback
  alone). `hostchannel.Target` enforces it.
- **The credentials file is the crown jewel** (`~/.config/sparkflow-agentd/credentials.json`):
  0700 dir, 0600 file, atomic writes, refused when group/other-readable. A rotated refresh
  token is persisted BEFORE the new access token is used, or the next restart cannot sign in.
- **Never print a token, a refresh token or an authorization code** — not in output, not in
  an error, not in a test log. Only the sign-in URL is shown.
- **Concurrency is capped** per person and per host.
- **The daemon never *authorises* projects**: whether this human may touch this project was
  decided upstream, by the only service that knows about projects. It enforces only "this is
  my owner" and its own allow-lists.
- **`init` takes the same deployment flags as `sparkflow-sync`** (owner, 2026-10-06):
  production by default, `--debug-dev` switches server + issuer + client id together. An
  empty client id is "not provisioned", said in a sentence — never the other deployment's.
- **Every ATTEMPT is audited** with the verified identity — refusals and errors as well as
  successes, and reads (`Snapshot`, `Stream`, `List`) as well as writes. A success-only log
  is blind to exactly the traffic worth reading: somebody enumerating kinds or probing
  `/srv/agents/../../etc` produces no line at all. Env NAMES go in the log, never env
  values.
- **Stop kills and leaves the workspace alone.** Uncommitted work stays on disk.

## Layering

`Handler → Application → Domain ← Infra`, enforced by `internal/archtest`. The
`applicationThirdParty` allow-list is **empty on purpose** — the use cases reach tmux, the
filesystem and the IdP only through ports. `init`'s ports are in `domain/host`; the adapters
from infra to them live in `cmd/sparkflow-agentd/adapters.go`, because infra may not import
application or handler (the gate caught `infra/hostchannel` importing `handler/channel` once). Adding an entry is a design decision; say why in the PR.

**This repo's copy of `layering_test.go` also judges the HANDLER layer**, which the shared
version does not: `Handler → Application → Domain` says nothing about handler → infra, so a
handler could import any adapter and the gate stayed green — as one did, defining its
`Verifier` port in terms of an infra struct and forcing every stub to import infra too. The
same hole exists in the other Go repos; see [[Layering Gates]].

## tmux, and what it does that surprises people

Measured on tmux 3.4, not recalled:

- **Control mode** (`tmux -C attach`) pushes `%output` as bytes appear — no polling. Its
  stdin must be **held open**: a control client reads commands from stdin and exits on EOF,
  so an attach with stdin at `/dev/null` produces a silently empty stream.
- `%output` is escaped as **backslash + three octal digits** (the backslash itself is
  `\134`). **UTF-8 passes through raw.**
- Replies are framed `%begin <ts> <num> <flags>` … `%end`/`%error` and correlate by
  **number**, never by arrival order.
- A detached session without `-x/-y` is **80×24**.
- `send-keys` needs `-l` for literal text, or a prompt containing the word `Enter` is typed
  as a keystroke.
- **`-t <name>` matches by PREFIX**: `kill-session -t sfagent-abc` killed `sfagent-abcdef`.
  The `=` exact-match form fixes it only for target-SESSION commands (`has-session`,
  `kill-session`); the target-PANE ones reject it (`send-keys -t =name` → "can't find
  pane") and `set-option` and `display-message` do too — the latter printing nothing and
  exiting **0**. So every pane-scoped command here addresses the **pane id** (`%0`), which
  tmux never reuses, and session ids are fixed-length so no valid name is a prefix of
  another.
- **A format string escapes control bytes in its OUTPUT**: a 0x1f field separator comes
  back as the four characters `\037`, so a packed `list-sessions -F` line parses as ONE
  field and the whole list silently reads as empty. Labels are read one at a time with
  `show-options -q -v`, which returns the raw value. A fake tmux writing real separator
  bytes said the packed version worked; the real one disagreed.
- **`capture-pane` needs `-e`** to keep escape sequences, or a snapshot and the live stream
  that follows it disagree about what the screen looks like.
- **`%exit` means this attach is over, not necessarily the session**: ask `has-session`
  before concluding the agent is gone.
- The raw server socket protocol is unversioned — never use it.

## Testing

Every change goes through a **pull request into `develop`**; `main` and `develop` are
protected: PR required, no force-push, and the CI jobs `test` and `tmux` are **required
checks** — a red PR cannot be merged. CI runs the same gates as below.

```sh
make gates                       # build + test + gofmt + vet
make race                        # the control-mode fan-out is proved here
make tidy-check                  # a published module's go.mod must be honest
go test -race -tags tmux ./internal/infra/tmux/ -count=1   # needs a real tmux
```

The control-mode adapter has unit tests against a **fake tmux** — this test binary
re-executed with `AGENTD_FAKE_DIR` set, playing a scripted attach. That is how a 2 000-line
burst, a `%output` for a pane we do not own, a `%error` and an attach that dies are all
testable without a real tmux. It is not a substitute for the tagged test: the fake once
agreed that a packed `list-sessions` format worked, and the real tmux did not.

The tmux integration test is **tagged in, not skipped**, so "0 tests ran" is never mistaken
for "the adapter works". It must leave no session behind; a leaked `sfagent-*` is a failure.
