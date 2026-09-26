# sparkflow-agentd

A daemon that owns **CLI-agent sessions on a host** — `claude`, `opencode-go`, `qwen`,
`kimi`, `z.ai` and friends — each running in its own `tmux` session, and exposes them
over gRPC.

```sh
go install github.com/sparkflow-space/sparkflow-agentd/cmd/sparkflow-agentd@latest
```

It is deliberately **dumb**: it knows sessions, PTYs, bytes and an audit line per command.
It knows nothing about companies, projects or permissions — those belong to the service
that calls it, which is the only thing that should.

## Security, in three lines

- Every RPC requires a **Zitadel JWT belonging to a person** — signature, expiry, issuer
  and audience are all checked. The daemon authenticates its caller; it never authorises.
- `kind` picks a binary from **the daemon's own allow-list**; a caller cannot pass a
  command line. `workspace` must sit under a configured root.
- It refuses to bind `0.0.0.0`. This process runs other people's code on your host.

## Status

Early. Contract and design: `AGENTD-001` / "CLI Agent Sessions" in the project vault.
