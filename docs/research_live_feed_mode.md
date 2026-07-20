# C++ Research Live-Feed Replay

Status: implementation brief  
Target binary: `research`  
Primary input: materialized historical run tables (`tables/*.parquet`)  
Primary objective: one deterministic, continuous, causal market-making replay with persistent simulated orders and inventory

## 1. Why this mode exists

The current research paths answer useful marginal questions, but they do not reproduce one continuous session-level order and inventory path.

- `cpp/apps/research/main.cpp` reconstructs books chronologically and exports episode/state panels.
- `python/eda/queue_fill_research.py` places many independent counterfactual orders at sampled states.
- `python/eda/dynamic_cancel_research.py` and `python/eda/fill_hazard_research.py` compare frozen entry/cancellation policies over those counterfactual orders.

Those paths can measure whether a state is associated with a later adverse fill. They cannot cleanly distinguish:

1. a quote that was already bad when posted;
2. a quote that became bad while resting;
3. a quote that was correctly cancelled but filled during cancellation latency;
4. an acceptable fill that became costly because inventory accumulated;
5. a fill whose apparent loss is only broad chain repricing.

The live-feed replay must maintain exactly one evolving simulated world per run. Historical events arrive in recorded order, just as a live feed would, while simulated orders, queue position, fills, cash, and signed inventory persist between events.

“Live-feed” in this document does **not** mean connecting the research binary to Kalshi. It means presenting a historical Parquet stream to the strategy through a causal, live-shaped interface. Version one must not sleep, pace itself to wall time, contact the network, or place real orders.

## 2. Research decision this mode must support

The previous horizon and adverse-selection work has not demonstrated a reliable entry-time toxicity classifier. That is evidence, but not proof that conditional structure does not exist. The remaining bounded question is whether observable changes during the life of a resting order provide enough warning to improve held-out economic outcomes after a realistic cancellation delay.

The replay should let us compare:

- an always-present passive baseline;
- the current frozen static entry gate;
- later, a frozen dynamic cancellation policy;
- eventually, inventory-aware control.

The first implementation is complete before dynamic cancellation or inventory skew is added. Its job is to make the baseline causal, deterministic, inspectable, and economically accountable.

If pooled/session-disjoint and trailing event/volume-time features still fail to improve held-out replay EV, close the toxicity-detection branch. Retain the unconditional fill/markout curve as an estimate of the cost of providing liquidity and move the research focus to quote economics and inventory control.

## 3. Required scope for version one

Version one must:

- read a single materialized run chronologically from Parquet;
- reconstruct all selected monotonic-chain books continuously;
- maintain one simulated passive YES bid and one simulated passive YES ask per tradeable market;
- join the back of displayed queue at the order price;
- apply conservative cancellation allocation;
- reconcile book removals with public trades so execution is not double-counted as cancellation;
- support partial fills;
- keep orders fillable while cancellation is pending;
- use a configurable cancellation latency with a default of 100 ms;
- update cash and signed YES position after every fill;
- net buys and sells algebraically per market;
- calculate midpoint and executable marks at fixed horizons and at end of run;
- emit compact lifecycle/fill tables and a dossier for every fill with a negative selected-horizon markout;
- produce deterministic session-level summary metrics;
- keep memory bounded by live state and pending outcomes, not total input rows.

Version one must support two policies:

1. `always_quote`: quote both displayed YES touches whenever that market/side has no active order.
2. `frozen_entry_gate`: make the same placement decision through a frozen, pre-run policy artifact. No fitting or threshold selection may occur against the replayed run.

The first end-to-end milestone may land `always_quote` before the model artifact loader, but the design must not hard-code the strategy inside the simulated venue.

## 4. Explicit non-goals

Do not add any of the following to version one:

- a real websocket or REST connection;
- production OMS integration;
- wall-clock pacing;
- multiple worker threads;
- asynchronous model scoring;
- model training or threshold selection;
- reinforcement learning;
- inferred participant intent;
- inventory skew, cross-strike hedging, or forced flattening;
- a claim that immediate executable unwind is the strategy PnL;
- a full Kalman or covariance risk overlay;
- quoting NO contracts;
- continuously sampled wide CSV panels;
- deletion of old research artifacts;
- changes to the production `predex` binary.

The replay must track inventory from the beginning, but version one deliberately does not control inventory. Immediate executable liquidation is a diagnostic lower bound, not the inventory policy.

## 5. Architectural rule

The replay should resemble the ownership shape of the production application while remaining a single-threaded deterministic event loop.

```text
materialized Parquet run
          |
          v
   HistoricalFeed ------ routes/metadata
          |
          v
      ReplayEngine <--------- internal timer heap
          |
          +----> MarketState / ActivityTracker
          |
          +----> SimulatedVenue ----> FillEvent
          |            |                 |
          |            v                 v
          |       order lifecycle     Portfolio
          |                              |
          +----------> Strategy <--------+
          |
          +----> OutcomeTracker ----> OutputSink
```

Do not use actual queues or threads in version one. The interfaces should make ownership explicit and permit a later live implementation, but deterministic chronological behavior is more important than imitating concurrency.

`main.cpp` should become a thin composition root that:

1. parses the subcommand and configuration;
2. constructs the feed, state, venue, strategy, portfolio, outcome tracker, and sinks;
3. constructs `ReplayEngine` with references to those owners;
4. calls `run()`;
5. prints the final summary or error.

No book mechanics, model math, CSV/Parquet row formatting, or queue-position logic should remain in `main.cpp` after the extraction is complete.

## 6. Ownership boundaries

| Component | Owns | Must not own |
|---|---|---|
| `RunManifest` | validated paths, run identity, materializer metadata | open readers or mutable replay state |
| `HistoricalFeed` | Parquet cursors and next external event | books, orders, strategy, outcomes |
| `MarketState` | reconstructed books, sequence validity, route lookup, contemporaneous chain views | simulated orders or portfolio |
| `ActivityTracker` | rolling causal activity windows and trade/removal reconciliation signals | book truth or order fills |
| `ReplayEngine` | event chronology, simulated clock, call ordering | policy math, queue formulas, output formatting |
| `Strategy` | quote/cancel decisions and its own policy state | queue position, fills, cash, exchange truth |
| `SimulatedVenue` | active orders, queue position, partial fills, cancel timers, terminal states | fair value, toxicity labels, portfolio accounting |
| `Portfolio` | cash, signed YES positions, fill accounting | order queue state or future marks |
| `OutcomeTracker` | pending horizon observations and fill markouts | strategy decisions |
| `OutputSink` | streaming artifact writers and final summary serialization | business decisions |

State should be passed as immutable views or small value snapshots. Do not pass mutable maps from one owner to another.

## 7. Suggested source layout

Names may change slightly to fit existing conventions, but keep the responsibilities separate.

```text
cpp/apps/research/main.cpp

cpp/include/predex/research/
  activity_features.hpp          # already exists; retain
  run_manifest.hpp
  replay_types.hpp
  historical_feed.hpp
  market_state.hpp
  activity_tracker.hpp
  strategy.hpp
  simulated_venue.hpp
  portfolio.hpp
  outcome_tracker.hpp
  output_sink.hpp
  replay_engine.hpp

cpp/src/research/
  run_manifest.cpp
  historical_feed.cpp
  market_state.cpp
  activity_tracker.cpp
  strategy.cpp
  simulated_venue.cpp
  portfolio.cpp
  outcome_tracker.cpp
  output_sink.cpp
  replay_engine.cpp

cpp/tests/
  research_historical_feed_tests.cpp
  research_market_state_tests.cpp
  research_simulated_venue_tests.cpp
  research_portfolio_tests.cpp
  research_replay_engine_tests.cpp
```

Do not create one class per trivial struct. The separation above is an ownership map, not a requirement for maximal file count. Combining closely related declarations is acceptable when ownership remains obvious.

## 8. CLI and compatibility

Add a subcommand without silently replacing the existing episode generator:

```bash
./build/dev/research live-feed \
  --run-dir runs/predex-YYYY-MM-DD-HHMMSS-label \
  --output-dir research_summary/live_feed/<run-id> \
  --policy always_quote \
  --order-qty-contracts 1 \
  --cancel-latency-ms 100 \
  --cancel-ahead-weight 0
```

Recommended legacy spelling:

```bash
./build/dev/research episodes --run-dir <run-dir>
```

During migration, `research <run-dir>` may remain as an alias for `episodes`. Do not change the content or meaning of existing episode CSV outputs as part of the mechanical refactor.

Minimum live-feed options:

| Option | Default | Meaning |
|---|---:|---|
| `--run-dir` | required | Materialized run root containing `tables/` |
| `--output-dir` | required | Dedicated result directory; refuse accidental overwrite unless explicitly allowed |
| `--policy` | `always_quote` | `always_quote` or `frozen_entry_gate` |
| `--policy-artifact` | none | Required for a frozen learned policy |
| `--topology` | `monotonic_chain` | Initial event filter |
| `--order-qty-contracts` | `1` | Passive order size; one contract is 100 quantity lots |
| `--cancel-latency-ms` | `100` | Delay from cancel request to effective pull |
| `--cancel-ahead-weight` | `0` | Conservative fraction/weight allowing cancellations to advance our queue |
| `--fees-ticks-per-contract` | `0` | Explicit fee assumption, always written to summary |
| `--markout-horizons-ms` | `100,500,1000,5000,30000,120000` | Post-fill observation horizons |
| `--toxic-horizon-ms` | `1000` | Horizon used only to select negative-fill dossiers |
| `--overwrite` | off | Permit replacing an existing result directory |

Validate every option. Reject negative latency, non-positive quantity, cancellation weights outside `[0,1]`, a missing artifact for a frozen policy, and unsupported materializer schemas.

## 9. Input contract

Use `frames.parquet` as the canonical chronological spine. It already supplies one row per tape record and identifies the corresponding specialized table through `frame_kind`. Advance exactly one matching specialized cursor for each recognized frame.

Required tables:

- `tables/manifest.json`
- `tables/frames.parquet`
- `tables/deltas.parquet`
- `tables/trades.parquet`
- `tables/snapshots.parquet`
- `tables/snapshot_levels.parquet`
- `tables/market_routes.parquet`
- `tables/event_routes.parquet`

`lifecycles.parquet` may be read later but is not required for the first passive-book experiment. The relevant market close/tradeable metadata is in the route tables.

Validate before replay:

- the manifest says materialization verification succeeded;
- required files exist;
- supported schema/materializer version is present (current materializer version is 4);
- `record_index` is strictly increasing in `frames.parquet`;
- specialized rows match the frame record, event, and market identifiers;
- route indices are within the owning event's market count;
- no selected frame references an unknown route.

Fail loudly on contract violations. Do not silently skip mismatched rows and continue with a corrupted book.

The reader must stream row groups/cursors. Do not eagerly load an entire run or a whole snapshot-level table into memory.

## 10. Canonical event and time contract

Normalize the Parquet rows into a small event variant:

```cpp
using FeedEvent = std::variant<
    BookSnapshot,
    BookDelta,
    PublicTrade,
    MarketLifecycle
>;
```

Every event must carry at least:

- `run_id`;
- `record_index`;
- `recv_ts_ns`;
- exchange sequence;
- `event_id`;
- `market_id`.

Use recorded receive time as the simulated clock. Use `record_index` as the authoritative total order between external events. Do not reorder external events by exchange timestamp.

Internal actions such as cancel effectiveness are ordered by `(effective_recv_ts_ns, timer_sequence)`.

### Equal-time conservative rule

Before external event `E` at receive time `t`:

1. apply internal timers with effective time strictly less than `t`;
2. apply `E`, including any resulting simulated fills;
3. apply internal timers with effective time equal to `t`;
4. update portfolio/outcomes from fills;
5. expose the complete post-event state to the strategy;
6. enqueue new strategy actions; they cannot affect `E` retroactively.

Therefore a public execution observed exactly at the nominal cancellation-effective timestamp can still fill the order. This is deliberately conservative and must have a regression test.

If two external rows have the same receive timestamp, retain their `record_index` order and run the strategy after each row. An order placed after one row may participate only in later rows, never the row that caused its placement.

## 11. Market-state rules

For each market, retain full displayed bid/ask depth needed by active order levels plus cached BBO.

- A snapshot replaces the displayed book and establishes sequence truth.
- A delta adds/removes quantity at one price.
- Quantity must never become negative.
- A sequence gap or impossible delta marks the market unavailable.
- No new quote may be placed while the market is unavailable.
- The next valid snapshot re-establishes the book.
- A market lifecycle close/untradeable transition terminates simulated resting orders with an explicit reason.

A snapshot or gap received while a simulated order is active makes its historical queue position unknowable. Version one must terminate that order as `queue_unknown_snapshot` or `queue_unknown_desync`, then allow a new order only after valid state is re-established. Do not pretend the previous queue position survives.

An order remains at its original limit price when the BBO moves. Do not automatically move it with the touch. Repricing must be represented as cancel followed by a later new order.

## 12. Trade/removal reconciliation

A negative depth delta does not by itself identify a cancellation. It may be the book-side consequence of a public trade. Counting the delta as a cancellation and then applying the trade again would advance/fill our queue twice.

Retain the existing research approach in `activity_features.hpp`:

1. observe a negative removal keyed by market, side, and price;
2. hold it pending for a bounded causal matching window;
3. match a later public trade at the exact market/side/price up to available quantity;
4. treat matched quantity as execution depletion;
5. treat only expired residual removal as inferred non-trade depletion.

Initial matching bounds should remain explicit and configurable:

- maximum record gap: 8;
- maximum receive-time gap: 100 ms.

Pending removal is allowed as a causal activity feature, but it may not advance queue position until classified. Confirmed execution consumes displayed queue according to price-time rules. Expired residual removal uses the configured cancellation-allocation rule.

Discard pending removals for a market on snapshot/desync reset.

## 13. Simulated order lifecycle

Key active orders by `(market_id, quote_side)`, where `quote_side` is `bid_yes` or `ask_yes`. At most one non-terminal order may exist for each key.

Required states:

```text
ABSENT
  -> RESTING
  -> PARTIALLY_FILLED
  -> FILLED

RESTING / PARTIALLY_FILLED
  -> CANCEL_PENDING
  -> PULLED

any live state
  -> QUEUE_UNKNOWN / MARKET_CLOSED / END_OF_RUN
```

`CANCEL_PENDING` remains fillable until the cancel-effective timer fires. A partial fill during that interval reduces leaves quantity but does not cancel the pending pull. A full fill wins over the pending cancel and makes the later timer a no-op.

For each order retain:

- stable simulated order ID;
- strategy/policy ID;
- event, market, and side;
- limit price and original/leaves/filled quantity;
- decision, activation, cancel-request, cancel-effective, first-fill, full-fill, and terminal timestamps/record references;
- displayed quantity at join;
- initial/current queue ahead and queue behind;
- terminal reason;
- entry, pre-cancel, and pre-fill diagnostic snapshots or stable snapshot references.

Version one may activate new orders immediately after the strategy decision because only cancellation RTT has been fixed by measured operational evidence. Keep `entry_latency_ns` as a future configuration seam and record the zero-latency assumption in `summary.json`.

Do not overlap an old and replacement order in version one. A replacement may become `RESTING` only after the old order reaches `PULLED`, `FILLED`, or another terminal state.

## 14. Conservative queue model

Use the following baseline mechanics, matching the tested Python implementation in `python/src/predex/research/queue_fill.py`:

- On activation, join behind all currently displayed quantity at the order's price.
- Later additions at the same price join behind us.
- A trade at our price consumes queue ahead first, then fills our leaves quantity, then consumes queue behind.
- A trade through our price implies a full fill regardless of estimated queue ahead.
- A cancellation never fills us.
- With `cancel_ahead_weight=0`, inferred cancellations remove quantity behind us first. Only unavoidable excess can reduce queue ahead.
- A nonzero cancellation weight is a sensitivity analysis, not the baseline.
- Queue ahead, behind, leaves, and filled quantities must never become negative.

Normalize Kalshi aggressor semantics once at the feed boundary:

- aggressive YES buying consumes the displayed YES ask;
- aggressive NO buying consumes the economically equivalent displayed YES bid.

Do not infer a fill from a BBO disappearance alone. Only confirmed public execution depletion or an execution through the order price can fill the simulated order.

## 15. Strategy contract

The strategy sees only information available after the current event and any fills it caused.

Suggested interface:

```cpp
struct DecisionContext {
    ReplayStamp stamp;
    EventView event;
    MarketView updated_market;
    ActivityView activity;
    OrderView bid_order;
    OrderView ask_order;
    PositionView market_position;
    PortfolioView event_portfolio;
    std::span<const FillEvent> fills_from_current_step;
};

using StrategyAction = std::variant<PlaceOrder, CancelOrder>;

class IResearchStrategy {
public:
    virtual ~IResearchStrategy() = default;
    virtual void on_step(const DecisionContext&, ActionBuffer&) = 0;
};
```

Use a bounded reusable action buffer rather than allocating a new large vector on every tick.

Strategy rules:

- It requests actions; it never mutates venue or portfolio state directly.
- It cannot see future markouts or labels.
- It cannot report its own order as filled.
- It must receive current signed position even when version one ignores it.
- Policy artifacts must contain a schema/version, ordered feature names, coefficients/parameters, threshold, training run IDs, validation run IDs, and a content hash.
- Reject an artifact whose feature schema does not exactly match the compiled scorer.

### `always_quote`

After each external event, for the updated valid/tradeable market:

- if no live YES bid order exists and a positive displayed bid exists, place one contract at that bid;
- if no live YES ask order exists and a displayed ask above the bid exists, place one contract at that ask;
- otherwise do nothing;
- never request a policy cancellation.

Orders do not chase the touch. If an order fills or otherwise terminates, the strategy may place a replacement using the then-current post-event book. A replacement cannot participate in the event that terminated the previous order.

### `frozen_entry_gate`

Use exactly the same behavior, except each absent side is passed through the frozen entry scorer. Record both accepted and rejected decisions with score, threshold, and rejection reason. No run-local calibration is permitted.

## 16. Portfolio and accounting contract

Normalize every version-one fill into signed YES inventory.

- Buy YES: positive position and negative cash.
- Sell YES: negative position and positive cash.
- Buys and sells in the same market offset algebraically.

For example, selling three YES contracts and later buying two produces a net position of `-1`, not separate `-3` and `+2` buckets.

Use contracts as the accounting unit at the portfolio boundary; convert the feed's quantity lots with `100 lots = 1 contract`. Preserve integer lots inside the venue so partial fills remain exact.

Ignoring fees for notation:

```text
cash_delta_ticks = -contracts * fill_price_ticks   for buy YES
cash_delta_ticks = +contracts * fill_price_ticks   for sell YES
```

Track:

- cash in tick-contract units;
- signed YES position per market;
- gross and net event/chain position diagnostics;
- fill count and volume by side;
- maximum absolute position per market and event;
- midpoint equity when both sides of the book are valid;
- executable liquidation equity: long positions marked to bid, short positions marked to ask;
- unmarkable positions separately rather than silently assigning zero value.

Fees must be an explicit configuration field and copied into every summary. Fee-free output must say `fees_ticks_per_contract: 0`; it must not imply that the result is net production EV.

Do not call end-of-run marked equity “realized PnL.” Report cash, marked equity, and liquidation equity separately.

## 17. Markouts and outcome tracking

Default fill horizons:

- 100 ms;
- 500 ms;
- 1 s;
- 5 s;
- 30 s;
- 120 s.

For a YES bid fill at price `p`, midpoint markout at future midpoint `m` is `m - p`. For a YES ask fill it is `p - m`.

Executable unwind uses the future bid for long YES inventory and the future ask for short YES inventory. Keep it separate from midpoint markout.

Each horizon must record:

- requested horizon;
- actual elapsed receive time;
- availability;
- future bid, ask, and midpoint;
- midpoint markout;
- executable unwind mark;
- contemporaneous chain-fair movement if available.

Do not interpolate future observations or read forward to fabricate an exact horizon. Capture the first valid post-horizon state and record actual elapsed time. If the run ends first or the book is unavailable, mark the outcome unavailable.

Pending outcomes should be maintained in a min-heap and streamed once their final required horizon is observed. This keeps memory proportional to fills within the maximum horizon rather than all fills in the run.

A “toxic” fill means negative realized markout at the configured dossier horizon. It does not assert another participant's intent.

## 18. Output contract

Write outputs under the dedicated result directory, not into the run's materialized `tables/` directory.

Prefer Parquet/JSON over wide CSV:

```text
<output-dir>/
  replay_config.json
  summary.json
  orders.parquet
  fills.parquet
  decisions.parquet
  toxic_fill_dossiers.jsonl
```

### `replay_config.json`

Resolved configuration, input manifest hash, policy artifact hash, binary/version identifier, and all assumptions needed to reproduce the run.

### `summary.json`

At minimum:

- input/output identity and policy;
- replayed frames and receive-time range;
- selected events and markets;
- snapshots, deltas, trades, gaps, and resets;
- decisions accepted/rejected by reason;
- orders, partial fills, full fills, pulls, and queue-unknown terminals;
- fill contracts and fill rate by side;
- cancellation requests, fills during cancel latency, and successful pulls;
- cash, net positions, midpoint equity, executable liquidation equity, and unmarkable exposure;
- markout count/mean/median/downside quantiles at every horizon;
- expected markout per quote/order and per filled contract;
- number of independent events/markets contributing fills;
- peak live orders and high-water memory/state counters;
- elapsed wall time and replay throughput;
- explicit fee, latency, queue, and entry-latency assumptions.

### `orders.parquet`

One terminal row per simulated order. Do not emit one row per order per market event.

### `fills.parquet`

One row per partial fill, including position/cash immediately before and after, queue state before fill, policy state, and horizon outcomes.

### `decisions.parquet`

One row per place/cancel/reject decision. Do not emit no-op decisions.

### `toxic_fill_dossiers.jsonl`

One structured record per negative selected-horizon fill containing:

- order and fill identity;
- entry state;
- latest state before cancel/stay decision;
- pre-fill local book, queue, activity, and position state;
- neighboring/chain state and broad repricing diagnostics;
- controller action and cancellation-latency window;
- midpoint and executable outcome;
- subsequent quote suppression/re-entry references where available;
- a categorical attribution field such as `bad_at_entry`, `deteriorated_while_resting`, `filled_during_cancel_latency`, `inventory_amplified`, or `unclassified`.

Attribution is a diagnostic classification from recorded chronology, not proof of causation or intent. Keep the raw fields that allow the classification to be audited.

## 19. Determinism and causal invariants

These are hard requirements:

1. The same binary, input manifest, policy artifact, and configuration produce byte-equivalent normalized rows and identical summary values.
2. No random cancellation placement is used in the baseline. Sensitivity modes must use an explicit recorded seed.
3. No feature can use a row with a greater `record_index` than the decision row.
4. A model is loaded before replay and remains frozen.
5. Strategy actions caused by an event cannot affect that same event.
6. A book removal and its matching trade cannot both advance the same queue quantity.
7. Portfolio state changes only from emitted `FillEvent`s.
8. Outcome labels cannot influence strategy or venue state.
9. The event loop never uses wall-clock time for market decisions.
10. Output row identity includes run, event, market, order/fill, record index, and receive time.

## 20. Required tests

Add unit tests before running full sessions.

### Feed and chronology

- Specialized rows match the frame spine.
- Missing/mismatched rows fail loudly.
- Equal receive timestamps preserve record order.
- Internal timers strictly before an event fire first.
- A market event tied with cancel effectiveness is applied before the cancel.

### Book and recovery

- Snapshot replacement and BBO caching.
- Adds/removes at non-touch and touch levels.
- Negative depth is rejected.
- Desync disables quoting.
- Snapshot/desync invalidates active queue position.
- A valid snapshot restores quote eligibility.

### Queue and venue

- Join behind displayed depth.
- Same-price trades consume ahead before our order.
- Trade-through implies full fill.
- Partial fill reduces leaves exactly.
- Same-price additions join behind.
- Strict cancellation removes behind first.
- Excess cancellation can advance ahead but never fill.
- Pending cancel remains fillable for 100 ms.
- Full fill makes a pending cancel timer harmless.
- One live order per market/side is enforced.
- Replacement cannot fill on the event that created it.

### Trade/removal reconciliation

- Exact market/side/price trade match.
- Partial trade match leaves residual removal pending.
- Expired residual becomes cancellation once.
- Snapshot discards only that market's pending removals.
- Matched volume is never applied twice.

### Portfolio and outcomes

- Buy three and sell two nets to positive one.
- Sell three and buy two nets to negative one.
- Cash signs are correct for bid and ask fills.
- Partial fills update position and cash once.
- Long executable mark uses bid; short uses ask.
- Unavailable horizon remains unavailable.
- First valid post-horizon observation records actual elapsed time.

### End-to-end fixture

Create a small deterministic fixture covering two neighboring markets, snapshot, additions, removals, matched trades, a partial fill, a cancel-latency fill, and end-of-run marking. Assert the exact order/fill counts, signed positions, cash, and markouts. The fixture must run in the normal test target without requiring a large run directory.

## 21. Performance constraints

- Stream input; never hold all frames/deltas/trades in memory.
- Store only current books, active/terminal-not-yet-written orders, bounded activity windows, pending reconciliation, and pending markouts.
- Use row-group/batched Parquet writers.
- Avoid per-event filesystem writes and `std::endl` flushes.
- Avoid unbounded per-tick audit output.
- Reuse bounded buffers in the strategy and engine hot path.
- Measure and report frames/second and peak state counts.

Do not parallelize until the single-threaded result is deterministic and tested. A later optimization may partition independent events, but the output merge and per-event chronology must remain deterministic.

## 22. Implementation sequence

### Milestone 0: protect existing behavior

- Build the current `research` target and tests.
- Record a representative legacy invocation and output row counts.
- Add a subcommand parser while preserving `research <run-dir>` behavior.
- Do not mix semantic changes into this step.

### Milestone 1: mechanical extraction

- Move manifest, Parquet row/cursor, route, book, episode, and writer responsibilities out of the anonymous namespace in `main.cpp`.
- Keep the legacy episode outputs unchanged.
- Make `main.cpp` a composition/dispatch root.
- Build and compare the representative legacy counts after every extraction.

### Milestone 2: normalized historical feed

- Add `FeedEvent`, `ReplayStamp`, `HistoricalFeed`, and route views.
- Stream using `frames.parquet` as the spine.
- Add chronology and contract tests.

### Milestone 3: market state and simulated venue

- Add book ownership/recovery behavior.
- Port the tested conservative queue mechanics from Python into C++.
- Integrate `DepthRemovalReconciler` without double-counting.
- Add the full queue/latency test set.

### Milestone 4: continuous baseline

- Add `Portfolio`, `AlwaysQuoteStrategy`, `ReplayEngine`, and internal timers.
- Run the small end-to-end fixture.
- Run one real session and emit order/fill/summary artifacts.

### Milestone 5: outcomes and dossiers

- Add fixed-horizon markouts, executable marks, and negative-fill dossiers.
- Confirm bounded pending-outcome memory.
- Compare aggregate fill/markout direction with the existing Python queue model on a deliberately matched configuration. Exact counts may differ only when a documented chronology difference explains them.

### Milestone 6: frozen static gate

- Define/version the policy artifact.
- Load and validate it in C++.
- Compare `always_quote` and `frozen_entry_gate` on complete held-out sessions.

Do not add dynamic cancel logic until milestones 0 through 6 are stable. Dynamic cancellation is the next experiment, not part of constructing the trustworthy baseline.

## 23. Definition of done for the first experiment

The live-feed baseline is ready for research use when:

- legacy episode mode still produces its expected outputs;
- the normal C++ build and tests pass;
- the synthetic end-to-end fixture has exact expected accounting;
- two identical real-run invocations produce identical summaries;
- memory does not scale with total replay rows;
- all fills can be traced to queue mechanics and a source trade;
- all position changes can be traced to a fill;
- every negative 1-second fill has a reviewable dossier;
- always-quote and frozen-entry-gate policies run through the same venue/accounting engine;
- summary output clearly distinguishes markout, executable liquidation, and session marked equity;
- no future label or run-local fitted value enters a decision.

The experiment is **not** required to be profitable to be complete.

## 24. Follow-on experiments after the baseline

Run these in order and freeze each choice on training/validation sessions before touching test sessions:

1. Dynamic cancellation from trailing causal market state with 100 ms cancel latency.
2. One pooled, session-disjoint model with market/chain identity retained.
3. One trailing event/volume-time feature pass; do not use future-completed volume buckets.
4. Session/event-cluster confidence intervals on EV and fill retention.
5. Inventory caps without skew.
6. Inventory-aware reservation-price/size control.
7. Chain-aware risk and cost-minimizing inventory transfer.

Stop expanding toxicity features if no held-out policy improves economic EV while retaining enough fills. Classification accuracy alone is not a promotion criterion.

## 25. Relevant existing code and documentation

Read these before implementing:

- `cpp/apps/research/main.cpp` — current chronological C++ generator and the code to extract.
- `cpp/include/predex/research/activity_features.hpp` — causal activity windows and depth-removal/trade reconciliation.
- `cpp/tests/research_activity_features_tests.cpp` — existing reconciliation and activity invariants.
- `python/src/predex/research/queue_fill.py` — tested conservative queue formulas to port.
- `python/tests/test_queue_fill.py` — behavioral examples for trades, cancellations, partial fills, and latency.
- `python/src/predex/research/event_replay.py` — current streaming Parquet merge/reference behavior.
- `python/eda/queue_fill_research.py` — current independent-order experiment; useful mechanics, but do not preserve its independent-sample architecture.
- `python/src/predex/research/dynamic_cancel.py` — current frozen cancellation policy math; follow-on only.
- `python/eda/dynamic_cancel_research.py` and `python/eda/fill_hazard_research.py` — current selection/validation experiment; follow-on only.
- `python/src/predex/replay/materialize.py` — authoritative Parquet schemas and materializer manifest.
- `docs/fair_pricing_research_framework.md` — causal feature boundary, validation protocol, and separation of fair value from execution.
- `docs/predex-python.md` — materialization and Python research entry points.
- `docs/architecture.md` and `docs/ownership_invariants.md` — production ownership style to emulate conceptually, not code to reuse directly.

Some broad production documents describe aspirational or older strategy/OMS shapes. When documentation and the current source disagree, current source and tests are authoritative. Do not “fix” production components as part of this research task.

## 26. Handoff instructions for an implementation model

Use this document as the task contract. Before editing:

1. inspect `git status` and preserve unrelated user changes;
2. read every file listed in section 25 that is relevant to the current milestone;
3. state which milestone you are implementing;
4. make the smallest coherent patch for that milestone;
5. add or update tests with the patch;
6. build `research` and the test target;
7. report landed behavior separately from scaffolding and deferred work.

Do not attempt all milestones in one patch. Start with milestone 0, then milestone 1. Do not implement dynamic cancellation, inventory control, or production integration unless separately requested after the continuous baseline is verified.
