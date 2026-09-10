# Runtime Architecture

This document describes the runtime built by `cpp/apps/predex/main.cpp`. It is
an implementation map, not an aspirational design. Experimental model and
controller work remains outside the distributed runtime tree.

## Process topology

```text
                                      operator client
                                            |
                                   UnixCommandServer
                                            |
                                            v
                                      ControlPlane
                                 /       /    \       \
                                v       v      v       v
                         wire session  shards  OMS   venue sessions
                              |          ^      ^     /          \
                              v          |      | order REST  private WS
                            Router ------+      +----------------+
                              |
                              +-----------> MarketDataLogger -> tape

                     shards -> MonotonicArbStrategy -> OMS
```

The normal live process has these threads:

1. The main thread pumps `ControlPlane`.
2. The operator thread runs `UnixCommandServer`.
3. One market-data thread runs `KalshiWireSession` when enabled.
4. One router thread runs `Router`.
5. `runtime.shard_count` shard threads each run one `Shard`.
6. One market-data logger thread runs `MarketDataLogger`.
7. One strategy thread runs `MonotonicArbStrategy` when enabled.
8. One OMS thread runs `Oms` when either order transport is enabled.
9. One order REST thread runs `OrderRestSession` when enabled.
10. One private order-feed thread runs `KalshiOrderSession` when enabled.

Idle behavior is selected by `runtime.thread_polling`. `low_latency` keeps the
worker hot; `harvest` spins, yields, and then sleeps with bounded backoff.

## Composition and startup

`main.cpp` is the composition root. Startup proceeds in dependency order:

1. Load and validate `AppConfig`.
2. Allocate every queue and the fixed-capacity frame pool.
3. Build the immutable universe and install it in `ControlPlane`.
4. Enqueue the universe to every shard.
5. Acquire the per-config operator lock/socket. Startup fails if another
   process already owns that endpoint.
6. Start shards, router, and logger.
7. Conditionally start OMS, strategy, order REST, private order feed, and
   public market data.
8. Distribute the appropriate immutable universe view to each enabled owner.
9. Let `ControlPlane` derive readiness from component acknowledgements.

The process does not become trade-authorized merely because its components are
ready. `allow-trading` is a separate operator transition and succeeds only when
the required graph is ready.

## Market-data path

### Wire session

`KalshiWireSession` owns the public websocket and is the only producer that
acquires fresh `FramePool` slots. For each inbound frame it:

1. Captures a steady-clock ingress timestamp.
2. Parses enough envelope metadata to identify SID, sequence, type, and market.
3. Observes sequence continuity before applying the configured-market filter.
4. Resolves the immutable numeric route for configured markets.
5. Acquires a generation-stamped frame slot and copies the payload once.
6. Stamps the handle with universe, event, market, shard, SID, sequence, and
   latency metadata.
7. Publishes `MarketDataPathMessage` to the router.

Unknown lifecycle markets are intentionally filtered after sequence
observation. This is why an unknown-market count can rise without representing
transport loss.

### Router

The router consumes a variant:

```text
FrameHandle
MarketInvalidationBarrier
OrderBookSubscriptionInvalidationBarrier
```

Frame routing is numeric; it does not repeat market-ticker discovery. A normal
frame is sent to exactly one shard. When that shard queue is full, non-book
traffic can terminate at logger/recycle, while a lost order-book frame also
creates a market-specific invalidation barrier.

Barriers are ordered with the data path. If a barrier cannot enter its target
queue, the router latches it and stops consuming new input until delivery
succeeds. A subscription barrier is delivered to every shard before the router
reports the incident to `ControlPlane`.

### Shards

Each shard owns one `EventStore`; each event owns its markets and books. The
shard validates the handle's shard and universe identity before parsing or
applying it.

Book synchronization states are:

```text
awaiting initial snapshot
        |
        v
      usable
        |
        | integrity failure
        v
awaiting recovery snapshot
        |
        | validated replacement snapshot
        v
      usable
```

Deltas received while a book is unusable are ignored without mutating book
state. Snapshot application builds and validates replacement state before
committing it to the live market.

After a relevant event revision, the shard publishes an immutable
`MonotonicPairObservation` to strategy. An event or shard invalidation publishes
an unavailable message so strategy cannot continue from stale state.

### Logger and recycling

`MarketDataLogger` fans in normal shard-completed handles, router/logger-only
handles, and wire-session fallback handles. It writes a length-prefixed binary
tape and returns handles through its dedicated recycle queue.

Router, logger, and each shard have independent recycle SPSC queues. The wire
session is the sole recycle consumer and the only owner allowed to return a
slot to `FramePool`.

## Integrity and recovery

There are two recovery scopes:

- A SID-level order-book sequence gap is subscription-wide. The wire session
  emits `OrderBookSubscriptionInvalidationBarrier`, and every shard invalidates
  its installed books.
- Pool or queue loss after a market is known is market-local. The detecting
  stage emits `MarketInvalidationBarrier` for the exact event/market target.

Shard transitions produce `ShardMarketRecoveryRequired`. `ControlPlane`
forwards that observation to `RecoveryCoordinator`, which deduplicates the
incident, tracks attempt/timeout state, and enqueues `RecoverMarketIo`.

The wire session implements recovery with Kalshi's `get_snapshot` subscription
action. The request does not alter the existing order-book delta subscription.
Request acceptance/failure is correlated back to the coordinator. A tagged
replacement snapshot flows through the ordinary router/shard path; successful
application produces `ShardRecoverySnapshotApplied`, completing the incident.

Capacity remains an operational defense, not the recovery protocol. Pool and
queue high-water marks are exposed so a normal burst can be sized away, while
the invalidation path handles correctness when loss still occurs.

## Strategy and execution path

All related markets for one event share an affinity key and therefore one
shard. The shard can construct a coherent pair observation without a
cross-shard read.

`MonotonicArbStrategy` polls one input queue per shard. It validates universe,
source shard, event revision, observation age, book availability, market
ordering, depth, continuity, fees, and configured edge before constructing a
two-leg group intent. Strategy never reads shard-owned mutable books directly.

The intent flows through one SPSC queue to `Oms`. OMS owns:

- intent admission and configured limits;
- strategy portfolio and venue-balance views;
- group and leg execution state;
- submit/reject/fill/cancel transitions;
- bounded repair of incomplete groups;
- periodic portfolio-reconciliation requests;
- messages returned to strategy.

Accepted commands flow to `OrderRestSession`, which uses a persistent HTTP/2
session. REST acknowledgements and portfolio snapshots return on the dedicated
REST-to-OMS queue. Private websocket order/fill/position events use a separate
private-feed-to-OMS queue, preserving the one-producer/one-consumer invariant.

Kalshi does not provide atomic all-or-none execution across the group. The
group policy is therefore a local execution contract: if venue outcomes leave
an incomplete or unsafe group, OMS latches one incident and uses its bounded
repair policy rather than pretending the legs were atomic.

## Control plane and operator path

`ControlPlane` owns process lifecycle and the latest telemetry snapshot. It
pumps operator commands and component status queues, coordinates universes and
recovery, and determines whether required components are ready.

The Unix command server is local-only and uses a deterministic per-config
socket plus a sibling lock file. It uses bounded nonblocking I/O and supports
shutdown wakeup while a client is connected. `predexctl` is a short-lived
newline-delimited request/response client.

The operator surface is deliberately small:

- inspect status or full counters;
- allow or disable new trading;
- request cancel-all;
- request graceful or forceful shutdown.

## Time and observability

Internal latency spans use `std::chrono::steady_clock`, surfaced as nanoseconds.
The clock is monotonic and appropriate for in-process durations; it is not used
as an externally comparable wall clock.

Histograms cover wire service, wire-to-router, router service,
router-to-shard, shard service, ingress-to-book-apply, shard-to-logger, and
ingress-to-logger-write. Recovery duration has its own distribution. Counters
are split by channel wherever sequence or delivery semantics differ.

`counterstats` is a point-in-time fan-in rather than a transactional snapshot,
so adjacent stage totals may differ by the number of frames currently in
queues.

## Shutdown

An operator should disable trading and inspect OMS state before requesting a
graceful shutdown. The process then requests stop on optional transport,
strategy, OMS, router, shard, and operator threads. The logger is stopped last
and drains its inbound queues before returning.

Automatic public/private websocket reconnect and resubscription remains a
known gap. Until it is implemented, a disconnect removes readiness and should
be treated as an operator-visible fault rather than a transparent recovery.
