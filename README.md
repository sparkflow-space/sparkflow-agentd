# sparkflow-agentd

A daemon that owns **CLI-agent sessions on a host** — `claude`, `opencode-go`, `qwen`,
`kimi`, `z.ai` and friends — each running in its own `tmux` session, and exposes them
over gRPC.

```sh
# after the first release — until then use @develop, see Status below
go install github.com/sparkflow-space/sparkflow-agentd/cmd/sparkflow-agentd@latest
```

It is deliberately **dumb**: it knows sessions, PTYs, bytes and an audit line per command.
It knows nothing about companies, projects or permissions — those belong to the service
that calls it, which is the only thing that should.

## Security

This process runs other people's code on your host. Everything below follows from that.

- **Mutual TLS on the wire.** The daemon serves TLS 1.3 and requires a client certificate
  signed by a CA you name. It starts in plaintext only when `tls.allow_plaintext_loopback`
  is set *and* the listen address is loopback — because the caller's bearer token crosses
  this hop and grants code execution for its whole lifetime.
- **A person's Zitadel JWT on every RPC** — signature, expiry, issuer and audience. The
  daemon authenticates its caller; it never authorises about projects.
- **A session belongs to the person who started it.** Another caller's token cannot see it
  in `List` or touch it with `Send`, `Stream`, `Snapshot` or `Signal`; a foreign id answers
  `NOT_FOUND`, the same as one that never existed. Ownership is written onto the tmux
  session itself, so it survives a daemon restart the way liveness does.
- **`kind` picks a binary from the daemon's own allow-list**; a caller cannot pass a
  command line. `workspace` must sit under a configured root **and exist** — tmux silently
  falls back to its own working directory for a `-c` path that does not.
- **The environment is an allow-list too**, defaulting to `SPARKFLOW_`. Without one,
  `LD_PRELOAD`, `BASH_ENV`, `NODE_OPTIONS` or `GIT_SSH_COMMAND` would run the caller's code
  inside an allow-listed binary and make the `kind` rule a formality.
- **Concurrency is capped**, per person and per host.
- **Every attempt is audited** — refusals and errors as well as successes, with env names
  and never env values.
- It refuses to bind a wildcard address, and the address is parsed rather than
  string-matched: `[::0]` is a wildcard too.

## Configuration

```json
{
  "listen": "10.0.0.7:50077",
  "tls": {
    "cert_file": "/etc/sparkflow-agentd/server.crt",
    "key_file": "/etc/sparkflow-agentd/server.key",
    "client_ca_file": "/etc/sparkflow-agentd/clients-ca.crt"
  },
  "workspace_root": "/srv/agents",
  "kinds": { "claude": ["claude"], "qwen": ["qwen", "--tui"] },
  "env_allow_prefixes": ["SPARKFLOW_"],
  "max_sessions_per_owner": 8,
  "max_sessions": 32,
  "reconcile_seconds": 30,
  "auth": {
    "jwks_url": "https://id.example/oauth/v2/keys",
    "issuer": "https://id.example",
    "audience": "<project id>"
  }
}
```

Nothing secret belongs in this file's defaults or in source: the module is published, and a
published `path@version` is cached by `proxy.golang.org` forever.

## Development

This repository lives here, on GitHub. Changes go through a pull request into
`develop`; `main` moves only by a numbered release. CI is `.github/workflows/test.yml`:
build, test, gofmt, vet, `go mod tidy` drift, the race detector, and an integration job
against a real tmux.

## Status

Early — no release tag yet, so `@latest` resolves to the skeleton on `main` and **fails**
("module does not contain package"). Until the first release, install the current
development line with:

```sh
go install github.com/sparkflow-space/sparkflow-agentd/cmd/sparkflow-agentd@develop
```

Contract and design: `AGENTD-001` / "CLI Agent Sessions" in the project vault.
