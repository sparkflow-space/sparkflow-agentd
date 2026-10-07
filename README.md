# sparkflow-agentd

Runs CLI coding agents — `claude`, `opencode`, `qwen`, `kimi` and friends — **on your own
machine**, each in its own `tmux` session, driven from Sparkflow: press Play on a board card,
type into the session from the web or the tablet, let the chat agent instruct it.

The daemon **dials out**. It opens no port: it holds one outbound connection to Sparkflow, over
which the work for your sessions arrives. One device is one *host*; you can enrol as many
devices as you like (a laptop, a workstation, a server), and Sparkflow lets you pick which one
runs an agent.

## Install and enrol

```sh
go install github.com/sparkflow-space/sparkflow-agentd/cmd/sparkflow-agentd@latest
sparkflow-agentd init                 # production; add --debug-dev for the development deployment
```

`init`:

1. **signs you in** — it prints a link (and opens it when this machine has a browser). After
   you sign in, the browser comes back to `localhost` and the terminal carries on by itself.
   **Over SSH**, with the browser on another machine, that redirect reaches nothing: copy the
   address from the browser's address bar — or just the code in it — paste it into the terminal
   and press Enter;
2. **suggests a name** — `<OS> · <Country>`, e.g. `Ubuntu · Germany` (the country comes from
   your time zone, offline); Enter accepts it;
3. **finds the agents** whose programs are on your `PATH` — the host offers only those;
4. **registers this device** as one of your hosts and stores the sign-in in
   `~/.config/sparkflow-agentd/credentials.json` (directory `0700`, file `0600`);
5. **offers to run it with the system** — a systemd *user* unit on Linux (plus
   `loginctl enable-linger`, so it keeps running after you log out), a LaunchAgent on macOS.
   Decline, or pass `--no-service`, and it prints the commands instead.

Running `init` again on the same device keeps the same host (one host per device). `--new`
replaces the enrolment; `--name`, `--yes`, `--no-browser`, `--manual` and
`--server`/`--issuer`/`--client-id` do what they say (`sparkflow-agentd init -h`).

Then, in Sparkflow, choose a **working folder** for each project on this host (project settings
→ Agents): the folder picker browses your home directory. A git repository gets a fresh
worktree per session under `<repo>/.claude/worktrees/`; a plain folder is used as it is.

```sh
sparkflow-agentd status                  # who this device is enrolled to, and whether it runs
sparkflow-agentd service install|uninstall|status
sparkflow-agentd run                     # what the service runs; in the foreground for debugging
```

**A lost or retired device**: revoke it in Sparkflow → My hosts. Its connection is closed and
refused from then on; delete `~/.config/sparkflow-agentd/` on the device if you still have it.

## Security

This daemon runs code as you. What it refuses, and why:

- **It serves one person** — whoever ran `init`. Every operation names the person it is for,
  and anything naming someone else is refused here and logged, even though Sparkflow already
  routes only your work to your hosts.
- **A caller passes a kind, never a command line.** Kinds resolve against the daemon's own
  allow-list. **The environment is an allow-list too** (`SPARKFLOW_` by default): `LD_PRELOAD`,
  `BASH_ENV`, `NODE_OPTIONS` and friends would otherwise run arbitrary code inside an allowed
  binary.
- **A working folder must lie under your home, after symlinks are resolved, and exist.**
- **No listening port**; the bearer token travels only over TLS (plain HTTP is accepted for
  loopback alone, for a local development stack).
- **The credentials file is the crown jewel**: whoever reads it can run your agents on this
  device. It is created `0600` and refused if it becomes readable by others. Tokens and codes
  are never printed or logged.
- **Every attempt is audited** — refusals and errors as well as successes — to stdout (the
  journal, under the service).
- **Stop kills and leaves the folder alone**: uncommitted work stays on disk.

## Configuration

Optional: `~/.config/sparkflow-agentd/config.json` overrides the defaults.

```json
{
  "kinds": { "claude": ["claude"], "qwen": ["qwen"] },
  "env_allow_prefixes": ["SPARKFLOW_"],
  "max_sessions_per_owner": 8,
  "max_sessions": 32
}
```

A file's `kinds` **replace** the defaults (`claude`, `opencode-go`, `jcode`, `qwen`, `kimi`).
Nothing secret belongs in defaults or source: the module is published, and a published
`path@version` is cached by `proxy.golang.org` forever.

## Development

This repository lives here, on GitHub. Changes go through a pull request into `develop`;
`main` moves only by a numbered release. CI is `.github/workflows/test.yml`: build, test,
gofmt, vet, `go mod tidy` drift, the race detector, an integration job against a real tmux, and
a release dry run.

The host channel's contract is `api/proto/agent/v1/hostchannel.proto`, vendored from
`agent-sessions`. Design: "CLI Agent Sessions" in the project vault (AGENTD-001, AGENTD-003).
