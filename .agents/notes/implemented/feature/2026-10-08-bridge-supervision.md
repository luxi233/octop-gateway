# Agent Note: wechatauto channel supervises its bridge process

Status: implemented

## Problem

The wechatauto channel depends on a local `wechatauto-bridge` sidecar
(127.0.0.1:18765). The bridge had no supervisor: every time it died (crashed
while WeChat was writing its DB, or killed as a shell child process when the
spawning session ended), the channel went to `stopped`/disconnected and stayed
there until a human restarted the bridge by hand. This happened three times in
two days of live operation. OS-level fixes (Startup folder .bat, scheduled
task) help boot time but do nothing for mid-session death, and `schtasks`
requires privileges the runtime may not have.

## Decision

The channel supervises the bridge itself, opt-in via `bridge_cmd`:

- `WechatAutoConfig.bridge_cmd`: command line used to spawn the bridge when it
  is unreachable. Empty (default) disables supervision — the channel only
  reports the failure, preserving previous behavior.
- `start()`: a transport-level failure on the initial `/health`
  (`aiohttp.ClientError`/`OSError` = connection refused = process down)
  triggers `_restart_bridge`. HTTP errors never do: 401 means the bridge is
  alive and the token is wrong; 5xx means it is alive but broken — respawning
  cannot fix either.
- `_poll_loop`: after `_RESPAWN_AFTER_FAILURES` (3) consecutive transport
  failures, attempts a respawn, rate-limited by `_SPAWN_COOLDOWN_S` (60s) so a
  broken `bridge_cmd` cannot loop a spawn every backoff tick.
- On successful respawn the poll cursor resets to 0. A fresh bridge restarts
  its `seq` space; keeping the old cursor would silently starve the poll
  forever. The `_seen` LRU dedup absorbs replayed events.
- `_launch_bridge` spawns fully detached (`DETACHED_PROCESS |
  CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW` on Windows, `start_new_session`
  on POSIX, stdio to DEVNULL) so the bridge survives the host process's
  lifetime — the original failure mode was the bridge dying with its parent
  shell.
- `stop()` never kills the spawned bridge. The bridge is shared host
  infrastructure: other consumers (a Hermes plugin, the `recent` CLI) may hold
  it, and channel disable/restart must not take the host's WeChat link down.

## Alternatives considered

- **OS-level supervision only** (Startup folder / scheduled task): kept as a
  complement for cold boot, but it cannot cover mid-session death and the
  elevated-token requirement for `schtasks` was itself denied on the live
  host. Channel-level supervision works wherever Octop runs.
- **Restart the channel row instead** (PATCH enabled off→on): that re-registers
  the channel but only masks the dead bridge; the poll loop still has nothing
  to talk to.
- **Killing the spawned bridge on `stop()`**: rejected — the bridge is shared;
  the failure mode being fixed was precisely "child dies with parent".

## Consequences

- Channel self-heals from bridge death in ≤ (3 poll failures + spawn +
  health wait) ≈ tens of seconds, with no human step.
- `bridge_cmd` duplicates the `token` value in the command line — the two must
  match; documented in the config docstring and example.
- Cursor reset on respawn may redeliver events the bridge saw before dying;
  `_seen` dedup bounds this to a no-op for already-dispatched ids.
