# Ownership and Invariants

PredEx relies on ownership rules more than internal locking. Most mutable
objects are intentionally not thread-safe because exactly one thread owns each
one. Cross-thread communication occurs through bounded SPSC queues.

Breaking an ownership rule is a correctness bug even when the code appears to
work during a light run.

## Single-owner state

| State | Sole owner |
|---|---|
| Process lifecycle, readiness, active universes, recovery coordinator | Control-plane thread |
| Public websocket and subscription sequence baselines | Wire-session thread |
| Frame-pool allocation and recycling | Wire-session thread |
| Router pending barrier and routing telemetry | Router thread |
| One `EventStore`, its events, markets, and books | Corresponding shard thread |
| Strategy observations, active group view, and strategy portfolio cache | Strategy thread |
| OMS group/order state, reservations, venue portfolio, and repairs | OMS thread |
| Persistent HTTP/2 connection and REST in-flight requests | Order-REST thread |
| Private websocket subscriptions and parser state | Private-feed thread |
| Tape file and logger counters | Logger thread |
| Unix listen socket and connected operator client | Operator-server thread |

Readers must consume snapshots or typed messages. They must not retain a
pointer into another owner's mutable state.

## Immutable universe views

`ControlPlane` owns the active universe version. It distributes immutable
`shared_ptr<const ...>` snapshots tailored to each component:

- market-data routing/subscription data to the wire session;
- event/market installation commands to shards;
- order-route metadata to OMS, order REST, and private order feed.

Messages and handles carry a universe version. A receiving component validates
that version before applying work. Universe replacement must not make an old
handle valid against new routing state.

## SPSC topology

Every queue must have exactly one producer thread and one consumer thread.
Fan-in uses multiple SPSC queues; fan-out uses one SPSC queue per destination.

### Operator and control

| Queue | Producer | Consumer |
|---|---|---|
| `server_to_control` | operator server | control plane |
| `control_to_server` | control plane | operator server |
| `control_to_io` | control plane | wire session |
| `io_to_control_status` | wire session | control plane |
| `router_to_control` | router | control plane |
| `logger_to_control_status` | logger | control plane |
| `control_to_shard[i]` | control plane | shard `i` |
| `shard_to_control[i]` | shard `i` | control plane |
| `control_to_oms` | control plane | OMS |
| `oms_to_control_status` | OMS | control plane |
| `control_to_order_rest` | control plane | order REST |
| `order_rest_to_control_status` | order REST | control plane |
| `control_to_private_order_feed` | control plane | private feed |
| `private_order_feed_to_control_status` | private feed | control plane |

### Market data

| Queue | Producer | Consumer |
|---|---|---|
| `wire_to_router` | wire session | router |
| `router_to_shard[i]` | router | shard `i` |
| `wire_to_logger` | wire session | logger |
| `router_to_logger` | router | logger |
| `shard_to_logger[i]` | shard `i` | logger |

`wire_to_logger` is a fallback path for a frame that cannot enter the router;
it preserves raw capture when possible but does not repair the missing book
application. Order-book loss still emits an integrity barrier.

### Recycling

| Queue | Producer | Consumer |
|---|---|---|
| `router_recycle` | router | wire session |
| `logger_recycle` | logger | wire session |
| `shard_recycle[i]` | shard `i` | wire session |

These queues cannot be combined without replacing the SPSC implementation with
an MPSC design. Each terminal producer owns one return path; the wire session
round-robins over all of them.

### Strategy and execution

| Queue | Producer | Consumer |
|---|---|---|
| `shard_to_strategy[i]` | shard `i` | strategy |
| `strategy_to_oms[0]` | strategy | OMS |
| `oms_to_strategy[0]` | OMS | strategy |
| `oms_to_order_rest` | OMS | order REST |
| `order_rest_to_oms` | order REST | OMS |
| `private_order_feed_to_oms` | private feed | OMS |

REST and private websocket events intentionally use different queues. Having
both transports push into one queue would violate the single-producer rule.

## Frame-pool lifecycle

`FramePool` preallocates fixed-size slots. The wire session is the only owner
allowed to call `try_acquire` or recycle a slot.

For a normal market-data frame:

1. The wire session acquires a slot and copies the payload.
2. It stamps `FrameHandle` with pool index and generation plus routing identity.
3. Router and shard read the slot through the handle; neither mutates pool
   ownership.
4. Logger writes the payload to tape.
5. The terminal stage pushes the handle onto its dedicated recycle queue.
6. The wire session validates and recycles the slot.

A handle is valid only while both its slot index and generation match. Every
terminal path must either hand off the handle or report a leak. A failed book
delivery must additionally invalidate the affected state.

## Market-data ordering

Sequence observation occurs in the wire session before configured-market
filtering. This preserves the exchange subscription's SID sequence domain.

The router queue carries frames and integrity barriers in one variant so the
barrier is ordered with the loss it describes. The router does not consume new
input while a barrier or its control-plane recovery fact is pending delivery.

A subscription-wide barrier is pushed to every shard in index order. It is not
reported as delivered until all shard queues accepted it.

## Shard and book invariants

- One event and all of its related markets are installed on one shard.
- A frame's shard, event, market, and universe identity is validated before
  parsing/application.
- A usable book becomes unusable exactly once per incident.
- Deltas never mutate an unusable book.
- A replacement snapshot is constructed and validated before it replaces live
  state.
- A recovery snapshot must match the expected market/universe/recovery
  identity.
- Event revision increases only after an accepted state mutation.
- Strategy receives immutable value messages, never references to shard state.
- When event/shard state becomes unavailable, strategy receives an explicit
  unavailable message.

## Recovery invariants

- SID sequence gaps and market-local delivery losses remain distinct facts.
- A subscription gap invalidates all installed books affected by that
  order-book subscription.
- Pool, wire-to-router, or router-to-shard loss invalidates the known target
  market.
- `RecoveryCoordinator` is the sole owner of incident deduplication, attempts,
  timeouts, and terminal outcome.
- At most one active recovery incident owns a market at a time.
- `RecoverMarketIo` requests a fresh snapshot; it does not replace or mutate
  the delta subscription.
- Recovery completes only after the shard accepts the correlated replacement
  snapshot, not when the websocket command is merely acknowledged.

## Strategy invariants

- Strategy consumes observations only from its configured universe and source
  shard.
- Stale revisions and observations older than the configured age are rejected.
- A strategy intent is a value message containing all execution inputs needed
  by OMS; OMS does not read strategy internals.
- Strategy may cache OMS-published portfolio state but cannot mutate the OMS
  portfolio.
- An event-unavailable or shard-unavailable message removes the corresponding
  observation from eligibility.

## OMS invariants

- OMS is the only writer to group and order execution state.
- Trading authorization and trading-session phase are checked before accepting
  new risk.
- A venue portfolio reconciliation is required when order REST is part of the
  configured graph.
- Reservation occurs before a venue command is published.
- Terminal or repaired outcomes release/convert reservations through OMS.
- Duplicate fills are idempotently ignored.
- Transport acknowledgements do not override contradictory private-feed or
  reconciliation evidence.
- An incomplete multi-leg group is an execution incident; it is not reported
  as an atomic success.
- Repair attempts are bounded by configuration and remain visible in
  telemetry.
- Unknown or non-tradeable market targets fail closed.

## Operator and socket invariants

- A file-backed config owns one deterministic socket path.
- A sibling lock file prevents two processes from starting against that same
  operator identity.
- Only a verified stale Unix socket node may be removed before bind.
- Client reads and writes are bounded and support partial progress, `EINTR`,
  `EAGAIN`, timeout, hangup, and shutdown wakeup.
- `predexctl` transport success is not the same as command acceptance; callers
  must inspect the JSON response.

## Shutdown invariants

For an operator-initiated routine stop:

1. Disable new trading.
2. Inspect live, pending, and uncertain OMS state.
3. Cancel orders if required.
4. Request graceful shutdown.
5. Stop network producers and internal consumers.
6. Stop the logger last so accepted handles can drain to tape.

The process must never silently abandon an uncertain order or claim a clean
flat shutdown solely because its local transport thread exited.

## Documentation rule

When a component owner, queue direction, state transition, or failure scope
changes, update this file together with [Architecture](architecture.md),
[Data Contract](data_contract.md), and [OMS Design](oms_design.md).
