# Design Decisions

This file records the reasons behind the runtime's non-obvious structural
choices. Current topology and ownership are documented separately in
[Architecture](architecture.md) and
[Ownership and Invariants](ownership_invariants.md).

## Bounded SPSC queues between owners

Each steady-state queue has exactly one producer and one consumer. This keeps
hot-path synchronization to the acquire/release publication boundary and makes
ownership visible in the composition root.

When several producers need to reach one consumer, PredEx uses one SPSC per
producer and consumer-side fan-in. Examples are shard-to-strategy, shard-to-
control, shard-to-logger, and the independent REST/private-feed event queues
consumed by OMS.

The trade-off is more explicit wiring. That is intentional: adding a second
producer to an existing queue should be a topology change that cannot hide
inside a convenience API.

## A separate router thread

The wire session already must parse envelope metadata for sequence integrity
and market attribution. The router still owns downstream fan-out, barrier
ordering, and shard backpressure so the wire session does not know every shard
queue or their delivery state.

Routing after the wire boundary is numeric. Ticker lookup is performed once,
then the handle carries shard/event/market indices. This keeps the single
router's work bounded and leaves full payload parsing parallelized across
shards.

## Observe sequence before filtering

Kalshi sequence numbers belong to a websocket subscription SID, not to an
individual configured market. Filtering an unknown lifecycle market before
sequence observation would create an artificial hole in the locally observed
subscription.

The wire session therefore parses SID/sequence/type/market envelope metadata,
updates sequence state, and only then filters markets outside the active
universe. Telemetry distinguishes intentional filtering from delivery loss.

## Recovery scope follows the known loss scope

A SID order-book sequence gap cannot identify which market was omitted, so it
invalidates every book on that subscription. A queue or pool loss after route
resolution identifies one affected market and uses a market-local barrier.

Collapsing these into one generic “desync” either leaves suspect books usable or
causes unnecessary global resets. Separate barrier types preserve the evidence
available at the detection point.

## Snapshot replacement, not delta patching

Once a book becomes unusable, further deltas are not safe inputs. The shard
ignores them until a correlated fresh snapshot arrives. Snapshot application
constructs and validates replacement state before committing it to the live
market.

Recovery uses Kalshi's `get_snapshot` subscription action because it obtains a
fresh book without changing the existing delta subscription or its SID.

## Fixed-capacity frame pool

Inbound payloads are copied once into a preallocated frame slot. Router, shard,
and logger pass a generation-stamped handle instead of allocating/copying the
JSON at every boundary.

The pool bounds memory and allocator jitter, but exhaustion means a frame was
lost. Capacity is therefore accompanied by an integrity protocol: loss of a
book-affecting frame invalidates that book and triggers snapshot recovery.
Increasing capacity handles expected bursts; it does not weaken the loss
contract.

## Event affinity and shard-local books

All markets belonging to one event share a stable affinity key and land on the
same shard. Event-level topology and monotonic comparisons can then be computed
against one coherent owner without locks or cross-shard reads.

The Python discovery layer derives stable event, market, and affinity identity.
The runtime still validates every stamped handle against the installed
universe before mutation.

## Publish strategy observations by value

Strategy runs on a separate thread and never reads a shard's mutable event
store. Shards publish bounded immutable observations carrying event revision
and timestamps. Explicit event/shard-unavailable messages revoke cached state.

This adds one value-copy boundary, but avoids locks and prevents strategy from
observing a half-applied event revision.

## One OMS writer

REST acknowledgements, private websocket fills, reconciliation snapshots, and
strategy intents can race in wall-clock time. They enter OMS through separate
SPSC queues, but only the OMS thread mutates order/group/portfolio state.

This centralizes identifier correlation, fill deduplication, capital
reservation, and group repair without placing locks on every order record.

## Local all-or-none groups plus repair

Kalshi batch submission does not make multi-order fills atomic. PredEx uses
`kALL_OR_NONE` to mean all legs pass OMS admission and are submitted as one
batch. Venue outcomes can still be partial.

OMS therefore models the group independently from its orders, tracks residual
exposure, cancels remaining legs, and issues bounded reduce-only repair orders
when necessary. An incomplete group is an execution incident, never silently
reported as a completed arbitrage.

## Reconciled venue capital and strategy allocation are distinct

Venue available balance is account-wide external truth. Strategy allocation is
an internal risk budget. OMS requires both constraints to pass and retains a
configurable venue safety reserve.

Mixing them into one number would let local accounting overwrite a newer venue
snapshot or let unrelated account activity escape the strategy limit.

## Monotonic time for latency

Latency spans use `std::chrono::steady_clock`. A realtime/wall clock can jump
because of NTP or manual correction and is therefore unsuitable for duration
measurement.

Steady timestamps are process-local. Correlation across machines or with
exchange timestamps requires a separate wall-clock field and clock-quality
model; it must not reuse these duration stamps.

## Tunable idle polling

Always spinning minimizes wake-up latency but consumed entire cores and drove
development hardware to its thermal limit during long harvest sessions.

The runtime therefore supports two policies:

- `low_latency` keeps critical loops aggressive for short live sessions;
- `harvest` transitions from spin to yield to bounded sleep when idle.

The message-processing code is the same in both modes. Polling policy changes
latency/power behavior, not correctness or queue semantics.

## Per-config operator sockets

A process-global default socket cannot distinguish concurrent PredEx instances.
Generated file-backed configs receive a deterministic socket path and the
server takes a sibling lock before binding. Starting a second process with the
same config fails instead of stealing the operator endpoint.

`scripts/predex-use` makes repeated terminal commands convenient by exporting
the config's socket path. `--socket` remains the explicit override for scripts
that require maximum targeting clarity.

## Separate production and research binaries

The production target does not link the historical replay/controller stack.
Research owns Parquet, simulation, counterfactual branching, and frozen model
artifacts behind a separate target and test binary.

This preserves a hard promotion boundary: deterministic or predictive research
success does not silently add a model to the live dependency graph.
