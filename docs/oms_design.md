# OMS Design

`Oms` is the single-writer coordinator between strategy and Kalshi's order
interfaces. It owns admission, execution state, capital accounting, group
repair, and the portfolio snapshots returned to strategy.

The implementation lives in `cpp/include/predex/oms/` and
`cpp/src/oms/oms.cpp`.

## Boundaries

```text
ControlPlane -------- control commands/status -------- Oms
Strategy ------------ typed intents -----------------> Oms
Strategy <----------- decisions/state/portfolio ------ Oms
Oms ----------------- REST commands -----------------> OrderRestSession
Oms <---------------- REST events/reconciliation ----- OrderRestSession
Oms <---------------- order/fill/position events ----- PrivateOrderFeed
```

OMS never reads shard books or strategy internals. A strategy intent is a value
message containing its universe/event/revision identity, all order fields, and
the timestamps needed for downstream latency attribution.

## Inputs

`StrategyIntent` is a variant of:

- `NewOrderIntent`
- `CancelOrderIntent`
- `ModifyOrderIntent`
- `GroupOrderIntent`

The live monotonic strategy emits a two-leg `GroupOrderIntent` with
`GroupAdmissionPolicy::kALL_OR_NONE`. This is an OMS admission policy, not a
claim that Kalshi provides atomic cross-order execution.

Venue facts arrive as `KalshiToOmsEvent` on two independent SPSC queues:

- order REST responses, portfolio snapshots, and REST-egress drain markers;
- private websocket order/fill/position events.

The separate queues preserve single-producer ownership.

## Readiness and trading authorization

OMS receives its immutable `OrderRouteUniverse` from `ControlPlane`. An intent
fails closed if its universe is stale, its market is unknown or non-tradeable,
or the OMS order graph is not ready.

New risk also requires:

- the process trading-session phase to permit it;
- explicit `AllowTrading` from the control plane;
- a reconciled venue portfolio when order REST is enabled;
- strategy and venue capital headroom;
- no active execution condition that blocks admission.

`DisableTrading` stops new admission. It does not rewrite venue truth or erase
live orders. `CancelAllOrders` is a separate explicit command.

## Identity and state

Every admitted order receives an OMS request ID and a deterministic client
order ID. Once known, the Kalshi exchange order ID is indexed as well. OMS can
therefore correlate REST and private-websocket facts that expose different
identifier subsets.

`OrderRecord` tracks:

- original intent context and group membership;
- order, outcome, action, liquidity, and time-in-force fields;
- ordered, filled, leaves, and working quantities;
- working price and venue identifiers;
- pending command kind;
- reserved capital and whether terminal accounting has run;
- whether the order was generated internally for repair.

Primary order states are:

```text
pending submit -> working -> partially filled -> filled
       |            |              |
       v            +-> pending cancel -> canceled
    rejected        +-> pending modify -> working
       |
       +--------------------------------> terminal

any unresolved transport/venue contradiction -> uncertain
```

Duplicate fill identity is tracked so the same execution arriving through REST
and private websocket cannot be charged twice.

## Group admission

Before publishing a batch, OMS validates the group as one admission unit:

- intent and leg counts are valid;
- group identity is not duplicated;
- every target belongs to the installed universe and is tradeable;
- the observation/intent age is within the configured maximum;
- required reservation fits `maximum_group_reservation_ticks`;
- the strategy allocation and reconciled venue balance have headroom;
- repair and execution policy allow new exposure.

On success, OMS creates the group and all leg records, reserves worst-case
capital, increments portfolio open-order/group counts, emits an accepted
`GroupAdmissionResponse`, and publishes one `SubmitOrderBatchCmd`.

Admission failure emits a rejected response and does not partially reserve or
publish legs.

## Group execution

Group states are:

```text
admitted -> pending venue -> working -> completed
                  |             |
                  |             +-> partially filled
                  |                         |
                  +-------------------------+-> repair required
                                                |
                              +-----------------+----------------+
                              v                                  v
                    canceling remainder                      unwinding
                              |                                  |
                              +--------------> flattened <-------+

unresolved venue state -> uncertain
exhausted/failed repair -> repair failed
no fills and no exposure -> aborted
admission failure -> rejected
```

Each group retains per-leg order state and confirmed residual YES-equivalent
exposure. If one leg rejects, expires, partially fills, times out, is canceled,
or becomes uncertain, OMS does not report the original package as completed.

Repair is bounded by `maximum_group_repair_attempts`:

1. Cancel any remaining original working quantity.
2. Reconcile what actually filled.
3. If residual exposure remains, issue OMS-generated reduce-only orders.
4. Mark the group flattened only after confirmed residual exposure reaches
   zero.
5. Latch an execution incident if safe resolution cannot be established.

This is the fail-safe substitute for venue-level atomicity.

## Portfolio accounting

OMS owns two related but distinct views.

### Venue portfolio

Periodic order-REST reconciliation supplies the account-wide available balance
and market positions. The snapshot has its own reconciliation identity and
timestamp. Venue balance is never synthesized from strategy estimates.

### Strategy portfolio

For each strategy, OMS tracks:

- configured allocation limit;
- available and reserved capital;
- inventory exposure;
- realized P&L and fees;
- open-order and active-group counts.

Per-market records track signed YES exposure, resting buys/sells, average entry
price, position cost, exposure, realized P&L, and fees. Positive position is
YES; negative is NO.

OMS publishes sequenced `StrategyPortfolioUpdate` and
`StrategyMarketPositionUpdate` messages. Strategy may use these as an
authoritative input but may not replace or mutate them.

Internally, money is represented in `$0.0001` ticks. Kalshi portfolio strings
may contain six decimal places and are rounded at the venue boundary according
to the field's conservative policy.

## Output messages

`OmsToStrategyMessage` includes:

- ordinary admission/rejection response;
- order-state update;
- group-admission response;
- group-state update;
- strategy portfolio update;
- strategy market-position update.

The response queue is bounded. Backpressure is counted and must not be treated
as proof that strategy observed an OMS state transition.

## Telemetry

The operator snapshot exposes:

- strategy intents received, processed, and rejected;
- Kalshi commands sent/failed;
- REST, private-websocket, and reconciliation events;
- reconciliation requests/completions/failures;
- duplicate fills and venue position updates;
- group repair attempts/commands/outcomes;
- response backpressure;
- live, pending-submit, and uncertain order counts;
- whether an execution incident is active;
- reconciled venue balance and last error.

Zero live orders is not sufficient for a clean shutdown if pending-submit or
uncertain orders remain.

## Known gaps

- The live strategy path has not yet accumulated enough real fill/repair
  outcomes to justify larger limits.
- Automatic private-websocket reconnect/resubscription and missed-event
  reconciliation remain to be implemented.
- Strategy candidate telemetry is not yet included in `counterstats`, so the
  operator cannot currently decompose zero intents into individual strategy
  rejection gates.
