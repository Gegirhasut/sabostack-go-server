# Hardening for high load

Changes made after the code review of the reference server. Every item has a test
(`internal/*/hardening_test.go`, `internal/store/redis_node_test.go`,
`internal/lobby/liveness_test.go`). All tests pass with `-race`.

## Gameplay and protocol fixes

| Problem | Fix |
|---|---|
| On resume, `player_back` reached the client **before** its snapshot, and again inside it. | `Match.Attach` attaches the new sink only after the snapshot is sent. The snapshot is always the first message. |
| A losing player could freeze a match forever by reconnecting just before `ReconnectGrace` expired (measured: 15 s of real time cost 0.25 s of round time). | `PauseBudget` (default 45 s, flag `-pause-budget`): each player's total time away while the match is paused. When it runs out, that player forfeits (`opponent_disconnected`). It is persisted in snapshots (`awayUsed`). |
| A client could send thousands of inputs per second, and all of them applied (measured: 42k in 5.5 s). | A token bucket on every connection (`-input-rate 40`, `-input-burst 20`). Inputs over the limit are dropped and counted as `inputs_total{result="rate_limited"}`. A client that keeps flooding is disconnected with error `rate_limited`. |
| Both players shared one actor command queue, so a flooder could crowd out the opponent's inputs. | Each seat has its own input queue (128). Overflow only drops the sender's own inputs. Commands (Attach, Detach) first drain the inputs queued before them, so ordering is preserved. |
| Both players idle meant seat 0 lost. | Both idle ends the match with no winner (`both_idle`). |
| A top-out inside `Input` was only noticed (logged, snapshotted) on the next tick. | The actor checks for a phase change after every step. |

## Resilience

| Problem | Fix |
|---|---|
| One panic in one match killed the process and every match on it. | `Actor.run` recovers. The match is aborted (`match_over`, reason `internal_error`), its clients are closed, and its snapshot is deleted so a restart does not crash on it again. Counted in `sabostack_actor_panics_total`. |
| A single snapshot writer, one round trip per write. Snapshots were dropped when it fell behind. | `PersistWorkers` shards (default 4, `-persist-workers`), each pinned to a set of matches, so writes for one match keep their order. Pending writes are **coalesced**: a newer snapshot replaces an unsent one (`snapshot_writes{result="superseded"}`), so nothing is dropped blindly. Batches of up to 64 writes go to Redis in one pipeline. |
| Snapshots carried the whole 256-event log (most of their size) every second. | Snapshots keep only the newest `SnapshotEvents` (32). A match whose state did not change is not re-snapshotted, except a forced refresh every 10 intervals. Size is exported as `sabostack_snapshot_bytes`. |
| With Redis down, match end blocked the match goroutine, and shutdown stopped matches one by one with no time limit. | The delete at match end is asynchronous (the TTL is the safety net). A circuit breaker skips writes for 5 s after 5 failed batches in a row. `Shutdown` stops all actors at once and never waits past its context. |
| Two servers on one Redis would each restore *all* matches (split brain). | Node mode: `-node-id` (and `-public-url`). Each match has an owner key, and saves and deletes are Lua scripts that refuse a match owned by another node (`ErrNotOwner`). A node that gets `ErrNotOwner` stops driving that match. Each node restores only its own set. Without `-node-id` the old single-node layout is used (with a warning). Single Redis or Sentinel only, not Redis Cluster. |
| A `resume` that reached the wrong node got `match_not_found`. | The node asks the store's directory, and replies `{"type":"error","code":"wrong_server","url":"wss://…"}` so the client can reconnect to the owning node. |

## Transport and ops

| Problem | Fix |
|---|---|
| A slow client was disconnected after 128 queued messages, including state frames that are superseded anyway. | `state` messages are conflated: only the newest unsent one is kept. Events, snapshots and errors are never dropped; 128 of *those* still means slow consumer. |
| `/metrics` on the public port, no pprof. | `-admin-addr` serves `/metrics`, `/healthz`, `/readyz` and `/debug/pprof/` on a private address. `-public-metrics` (default true) keeps `/metrics` on the public port, which the load bot uses. |
| No readiness or draining for rolling deploys. | `/readyz` returns 503 while draining. `-drain-timeout`: on SIGTERM, stop matchmaking (queued players get `unavailable`), let live matches finish, then snapshot the rest. Resumes keep working while draining. A second signal kills the process. |
| Any Origin accepted. No connection limits. | `-allowed-origins` (no Origin header, e.g. native apps, is always accepted), `-max-conns`, `-max-conns-per-ip`, `-trust-proxy` (client IP from `X-Forwarded-For`). The limits are off by default, so local load tests from one IP keep working. |
| Names were measured in bytes, and control characters were allowed. | 1–24 printable runes. |
| Bad field types (`"seq":"x"`) closed the connection. | They get `bad_request`; the connection stays open. |
| Matchmaking could pair a player with an opponent whose socket had already died. | `lobby.JoinContext` skips waiting tickets whose connection context is done. `Leave` is O(1) (a linked list). |
| Disconnects had no reason in metrics. | `sabostack_connections_kicked_total{reason}`: `slow_consumer`, `write_failed`, `ping_failed`, `pong_timeout`, `rate_limit`. Also `sabostack_connections_rejected_total{reason}` and `sabostack_persist_pending`. |

## Heartbeat: why the defaults stayed at 20 s / 30 s

A silently dropped mobile connection counts as connected for up to `PongWait`. The match
is not paused in that window, and with `PongWait` (30 s) equal to `IdleTimeout` (30 s) the
player can lose on idle before the drop is noticed. A 3 s ping and 10 s pong-wait were
tried. On a heavily loaded host (server plus 1,200 bots on one laptop) they disconnected
**live** clients that were only slow to read: 187 drops versus 0 with the old timings. So
the defaults keep the original values, and the timings are flags (`-ping-interval`,
`-pong-wait`).

How to choose:

- **Lower bound:** p99 of the silence of a *live* client: RTT, packet loss, app freezes,
  background-tab throttling (a background browser tab can stall timers for a minute).
  Measure it on real phones before tightening.
- **Upper bound:** keep `PongWait` below `IdleTimeout`. Otherwise a silent drop is judged
  as AFK.
- **Trade-off:** lower means faster pause on a real drop but more false drops (each one is
  a reconnect, a pause for the opponent, and budget spent). Higher means fewer false
  drops, but a vanished player keeps "playing" unpaused for longer.

## Load comparison (one Windows laptop, server and bots on the same 16 threads)

`cmd/bot -pairs N -rate 10`, server `-countdown 1s`, in-memory store.

| | before | after |
|---|---|---|
| 300 pairs: matches / errors | 300 / 0 | 300 / 0 |
| 450 pairs: matches / errors / drops | 441 / 18 / 0 | **450 / 0 / 0** |
| 450 pairs: input→ack p50 / p95 / p99 | 219 / 2283 / 3260 ms | **167 / 1283 / 1844 ms** |
| 450 pairs: mean tick | 2575 µs | **535 µs** |
| 600 pairs | the laptop is saturated for both versions (hundreds of refused TCP connects) | same |

Absolute latencies are high because native Windows plus 900–1,200 bot goroutines on the
same CPU is not a server. Compare the columns, not the numbers.

## Not done here (decisions or bigger projects)

- **Cross-node matchmaking.** The queue is still per process. Pairing players who
  connected to different nodes needs a shared matchmaker that allocates the match to one
  node, which can then redirect players with `wrong_server`.
- **Takeover of a dead node's matches.** Ownership expires with the snapshot TTL. Fast
  takeover needs a short lease that is renewed while the node is alive.
- **Bandwidth.** A `state` message is about 0.9 KB and carries both full boards, which
  works out to about 10 KB/s per client. Deltas and a binary encoding need client changes.
- **Sharded tick loops** instead of one ticker per match, if profiling ever shows
  timer overhead.
