# CLAUDE.md — sparkflow-agentd

The host daemon for [[CLI Agent Sessions]]: it owns CLI-agent processes (`claude`,
`opencode-go`, `qwen`, `kimi`, `z.ai`) as **tmux sessions** and serves them over gRPC to
exactly one caller, the in-cluster `agent-sessions` service.

**This repository is different from every other one in the estate in two ways. Both matter
before you touch anything.**

1. **It is published to GitHub.** Development lives here on GitLab, but the module path is
   `github.com/sparkflow-space/sparkflow-agentd` and a tag is mirrored out so
   `go install …@latest` works — the same track as `sparkflow-sync`, procedure in the
   `cli-release` skill. Two consequences: **nothing secret may live in source or defaults**,
   and **a published `path@version` is cached by `proxy.golang.org` forever** — a bad
   release is fixed by the next PATCH, never by moving a tag.
2. **It runs other people's code.** Everything below follows from that.

## The rules that are not negotiable

- **A caller passes a `kind`, never a command line.** Kinds are resolved against the
  daemon's own allow-list in the config file. This is the last place able to enforce it:
  the service above may be wrong or compromised.
- **A workspace must resolve under the configured root.** Validation is lexical and
  deliberately strict (`/srv/agents-evil` is not a child of `/srv/agents`).
- **It refuses to bind a wildcard address.** A private interface, always.
- **Every RPC carries a person's Zitadel JWT** — signature, expiry, **issuer and audience**.
  The daemon *authenticates* its caller and never *authorises*: whether this human may
  touch this project was decided upstream, by the only service that knows about projects.
- **Every command is audited** with the verified identity. A CLI agent can delete a
  repository; the log is how "who told it to" stays answerable.
- **Stop kills and leaves the workspace alone.** Uncommitted work stays on disk.

## Layering

`Handler → Application → Domain ← Infra`, enforced by `internal/archtest`. The
`applicationThirdParty` allow-list is **empty on purpose** — the use cases reach tmux and
the verifier only through ports. Adding an entry is a design decision; say why in the MR.

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
- The raw server socket protocol is unversioned — never use it.

## Testing

```sh
make gates                       # build + test + gofmt + vet
go test -tags tmux ./internal/infra/tmux/ -count=1   # needs a real tmux
```

The tmux integration test is **tagged in, not skipped**, so "0 tests ran" is never mistaken
for "the adapter works". It must leave no session behind; a leaked `sfagent-*` is a failure.
