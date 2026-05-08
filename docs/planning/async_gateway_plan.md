# Async Gateway Plan

## Goal

Define the target venue-gateway design before more C++ implementation work so the
runtime does not accrue churn around the wrong abstraction boundary.

This plan assumes:

- OMS command types stay stable.
- OMS remains the sole owner of canonical order state.
- The exchange gateway owns transport scheduling, batching, rate-limit admission,
  recovery, and session utilization.
- The current async transport prototype is disposable scaffolding rather than a
  contract to preserve.

## Why The Current Shape Is Wrong

The current `ExecutionTransport` seam is a queue topology adapter, not a venue
gateway contract.

Symptoms:

- REST routing is still expressed in terms of worker lanes.
- Private WS truth and REST command completion are modeled as separate worker
  systems rather than one venue gateway.
- Recovery and reconcile are bolt-on behaviors instead of first-class state.
- The async prototype still models one command as one request instead of letting
  the gateway derive batchable dispatch units.

The redesign should therefore preserve OMS command/event contracts where they are
already sound, but replace the exchange transport boundary with a gateway model.

## Stable Inputs And Outputs

### Keep

- existing OMS command types: `SubmitOrderCmd`, `CancelOrderCmd`, `ModifyOrderCmd`
- existing OMS/private-WS normalized event vocabulary where practical
- group lineage carried in `IntentContext` (`group_intent_id`, `leg_index`,
  `leg_count`)

### Change

- stop treating one OMS command as one HTTP request
- stop treating worker fanout as the transport abstraction
- make recovery, batching, and rate-limit admission explicit gateway behavior

## Reset Decision

The current async transport scaffolding should be treated as disposable.

### Keep from the current codebase

- OMS command vocabulary and queue ingress shape
- normalized venue-event vocabulary where it already matches OMS needs
- REST/private-WS codec knowledge in the transport layer
- auth/session machinery that can be reused behind new gateway interfaces

### Discard as target abstractions

- worker-lane mental model
- one-command-equals-one-request modeling
- client-local recovery or uncertainty decisions
- coordinator state keyed only by `OmsRequestId`
- async wrappers that merely run blocking REST calls in futures while preserving
  the old transport boundary

The new implementation should compile against the same OMS-facing command/event
contracts where possible, but should not preserve the internal types or class
boundaries of the current async prototype just to save code churn.

## Gateway Responsibilities

The gateway should own three planes.

### 1. Command plane

- accepts OMS commands
- sequences per-order traffic
- builds singleton or batched REST dispatch requests
- admits requests through a write-rate limiter
- assigns work to warm REST clients

### 2. Truth plane

- consumes private WS account/fill truth
- performs startup reconcile and reconnect repair
- resolves post-write-unknown requests
- performs open-order and order-history lookup by `client_order_id`

### 3. Codec/session plane

- builds REST payloads
- parses REST responses
- owns TLS/session reuse and auth signing
- owns private WS session mechanics
- does not know OMS policy or risk semantics

OMS should only see one logical gateway:

- one outbound command ingress
- one inbound normalized venue-event stream

## Private WS Role

Private WS should be treated as auxiliary truth, not as the primary order
lifecycle source of truth.

### What private WS is good for

1. fill and PnL attribution
2. account-side truth that may arrive faster than REST recovery
3. detecting reconnect/gap conditions that require repair

### What private WS should not be trusted to do alone

1. act as the primary driver of canonical order lifecycle state
2. replace REST/recovery-based resolution for post-write-unknown submits
3. serve as the only authority for startup adoption

Implication:

- the gateway should primarily resolve order lifecycle through REST completion or
   explicit recovery lookup/adoption
- private WS is an extra signal that can accelerate fill/PnL handling and hint
   that recovery may already be resolvable
- private WS should never by itself stop further recovery lookup for
   post-write-unknown requests

## Core Internal Components

### CommandIngress

Consumes existing OMS commands unchanged.

### OrderSequencer

Tracks per-order readiness and prevents overtaking.

Rules:

- submit, cancel, and modify for the same order lineage must not overtake
- sequencing is keyed by order lineage, not by client lane
- order readiness is blocked by in-flight or unresolved transport state

### BatchPlanner

Converts commands into dispatch requests.

### RateLimiter

Admits dispatch requests by transaction cost, not HTTP request count.

### SessionPool

Owns warm REST clients partitioned by scheduling class.

### RecoveryEngine

Owns retry and lookup logic for transport-unknown requests.

### TruthMux

Merges REST completions, private WS truth, and recovery results back toward OMS.

## Transport State Model

The gateway needs an explicit transport confidence model. The missing friction in
the async path is not merely “waiting for response”; it is that a request may
have crossed the write boundary without a final result yet.

### Request states

These apply to one dispatch request, which may contain one or many dispatch
items.

1. `queued`
   - accepted by the gateway
   - not yet assigned to a client

2. `admitted`
   - passed rate-limit admission
   - waiting for an eligible warm client

3. `dispatched_pre_write`
   - assigned to a client
   - session has not yet confirmed socket handoff

4. `post_write_unknown`
   - bytes were handed to the transport/session
   - final item-level outcome is not yet known
   - recovery is now required if no normal completion arrives

5. `completed`
   - a final REST response or other terminal resolution was obtained for every
     item in the request

6. `abandoned_to_recovery`
   - the request will not complete normally on the hot path
   - recovery engine now owns resolution

### Item states

Each dispatch request contains one or more dispatch items.

1. `pending`
2. `acked`
3. `rejected`
4. `unknown`
5. `adopted_from_truth`

Important distinction:

- `unknown` is a transport confidence state
- OMS `OrderUncertain` is a trading/order-state outcome

The gateway should only surface an OMS uncertain event when it decides the order
can no longer remain an internal transport-resolution problem.

Corollary:

- `post_write_unknown` should remain transport-internal by default
- session/client executors must not emit OMS-facing `OrderUncertain` directly
- only the gateway truth/recovery layer may escalate transport uncertainty into
  OMS-visible uncertainty

## Dispatch Units

The gateway should introduce a new internal distinction:

- `DispatchRequest`: one REST request, possibly batched
- `DispatchItem`: one logical OMS command carried by that request

This is the correct layer for batching, item-level response parsing, and
transaction-cost accounting.

## Identity Model

The gateway should define explicit internal ids before implementation begins.

### 1. `LineageKey`

Used for sequencing and readiness.

Purpose:

- identifies the mutable order stream that must not overtake itself
- survives retries, recovery, and adoption

Likely basis:

- initial submit lineage from OMS order identity
- then bound to `client_order_id`, and later optionally `exchange_order_id`

### 2. `DispatchItemId`

Used for one logical OMS command inside the gateway.

Purpose:

- one submit/cancel/modify logical action
- stable handle for item-level resolution inside batched requests

### 3. `DispatchRequestId`

Used for one outbound REST request.

Purpose:

- carries one or many dispatch items
- unit of rate-limit admission, session assignment, and post-write confidence

### 4. `GroupKey`

Optional grouping identity derived from existing `IntentContext`.

Purpose:

- batch planning for grouped submits
- coordinated cancel/unwind handling

This identity model should remain transport-internal. OMS should not need new
command types just to support it.

## Batching Rules

The gateway should batch only where it clearly reduces leg-skew risk.

### Batch now

1. grouped submit legs from one `GroupOrderIntent`
2. coordinated cancel waves such as hard-halt unwind or group unwind

### Do not batch yet

1. unrelated singleton submits that merely arrive close together
2. modify commands unless Kalshi exposes a true batch amend endpoint

Rationale:

- grouped submits share economic fate, so co-travel over the network is valuable
- unrelated orders do not justify the extra coupling, recovery complexity, or
  observability blur

### Batch planner rules

1. derive batchability from existing command lineage rather than adding new OMS
   command types
2. if commands share `group_intent_id` and represent submit legs, produce one
   batch create request
3. if commands are part of a coordinated cancel wave, produce one batch cancel
   request
4. otherwise produce singleton dispatch requests

## Retry And Recovery Policy

Retry behavior must be command-specific.

### Safe automatic retry

`SubmitOrderCmd` with the same `client_order_id`.

Basis:

- local Kalshi docs state `client_order_id` is idempotent
- duplicate submit should resolve as duplicate rather than double exposure

### Not automatically safe by default

- `CancelOrderCmd`
- `ModifyOrderCmd`

These should escalate to lookup/reconcile unless venue semantics are verified to
support equivalent idempotent retry.

### Recovery engine responsibilities

1. retry idempotent submit on reserved recovery capacity
2. lookup current open orders by `client_order_id`
3. lookup order history for already-terminal orders
4. adopt venue truth back into OMS via normalized events
5. retire stale transport state once an item is resolved

### Failure policy

1. drop before write
   - abort the offending request or grouped trade locally
   - do not retry because the venue never saw the request

2. drop after write with no response
   - treat as `post_write_unknown`
   - resubmit submit items through reserved recovery capacity with the same
     `client_order_id`

3. duplicate submit
   - treat as safe and expected under recovery
   - resolve via lookup/adoption rather than treating it as a fresh failure

4. restart with outstanding unknown submits
   - run the normal adopt strategy on startup
   - query current and past order state by lineage / `client_order_id`
   - switch on recovered outcome
   - if no order exists, abort locally and unwind any torn exposure if needed

## Reserved Capacity

The gateway should reserve warm REST capacity by class, not merely keep a bag of
equivalent clients.

### Required classes

1. hot submit clients
   - fresh submit/modify/cancel traffic

2. recovery client
   - idempotent submit retry
   - order lookup and history recovery

3. reconcile client or reserved reconcile capacity
   - startup/open-order snapshot repair

### Scheduling rules

1. hot traffic should not starve recovery
2. reconcile should not consume hot submit capacity under normal operation
3. recovery capacity may be borrowable by hot traffic only if recovery backlog is
   empty

## Rate Limiting

The rate limiter must operate on transaction units, not on HTTP request count.

Local venue notes state:

- create / cancel / modify each cost 1 write transaction
- each batch create item costs 1 write transaction
- each batch cancel item costs 0.2 write transactions

### Admission model

Use a token bucket with fixed-point transaction units.

Example for basic tier:

- capacity: 10 writes/sec
- internal scale: 10 units per write
- singleton create/cancel/modify: cost 10
- batch create of 3 legs: cost 30
- batch cancel of 3 legs: cost 6

### Admission rules

1. compute full dispatch-request transaction cost before dispatch
2. grouped requests must be admitted atomically or not at all
3. if headroom is insufficient for the whole grouped request, defer or reject the
   entire group; never partially send it because of local rate-limit exhaustion
4. consume tokens at socket handoff, not on response arrival
5. if a request is rejected before write, release the reservation
6. if a request becomes `post_write_unknown`, keep the reservation consumed

### Rate-limit failure policy

1. HTTP 429 on singleton or batch should be logged immediately
2. the offending request should be aborted rather than waiting a full second for
   token refill in the hot path
3. the current hot order queue should be flushed after 429 because waiting for
   the next refill window is too slow for the strategies this runtime cares
   about
4. this flush behavior should be explicit gateway policy, not an accidental side
   effect of scheduler backpressure

### Budget partitioning

Maintain at least:

- hot-path write budget
- reserved recovery budget

This prevents a burst of fresh strategy traffic from fully starving recovery.

## Grouped Trade Semantics

Grouped trades are where batching matters most.

The gateway should provide:

1. atomic rate-limit admission for the whole group
2. one batch create request so legs traverse the internet together
3. item-level resolution for partial venue success/failure inside a batch response
4. post-write-unknown recovery by leg via `client_order_id`

Important:

- batch transport reduces network skew risk
- it does not provide transactional all-or-none execution at the exchange
- OMS and recovery still must handle per-leg mixed outcomes

Boundary note:

- the gateway may synthesize transport-local follow-up work such as recovery
  lookups and policy-driven unwinds
- OMS remains the owner of canonical order state and risk accounting for the
  resulting normalized events
- if unwind generation is allowed inside the gateway, that must be an explicit
  policy surface rather than an accidental side effect of recovery code

### Partial batch success policy

Partial batch success should default to risk minimization.

1. if one or more legs are accepted and peers fail or remain unknown, treat the
   batch as torn
2. prefer immediate unwind of accepted legs to minimize directional or spread
   exposure
3. do not chase the missing legs by submitting the next legs after a true
   partial batch outcome; that increases risk and turns a torn trade into a
   larger one
4. exact unwind behavior may depend on why peer legs failed, but the default
   policy should still be unwind-first rather than continue-first
5. duplicate/lookup-safe legs should still go through normal recovery/adoption
   logic before declaring the batch settled

### Failure-reason defaults

1. hard reject on one or more legs
   - assume the bundled trade is no longer valid
   - immediately unwind accepted legs
   - this is the default policy for arb-style strategies

2. strategy-specific exceptions
   - some future strategies may intentionally tolerate partial bundled outcomes
   - that should be an explicit strategy policy override, not the gateway
     default

3. post-write-unknown on one or more legs
   - first use the reserved recovery connection to resubmit submit items with
     the same `client_order_id`
   - only unwind after recovery resolves to rejection or otherwise confirms that
     the intended bundled trade cannot be completed safely

### Unwind execution default

For the current runtime shape:

1. strategies are effectively entering with market-order-like intent rather than
   passive market-making intent
2. the safest default unwind is therefore a marketable limit intended to flatten
   accepted exposure quickly
3. passive cancel-only unwind behavior is not sufficient as the default because
   it assumes the accepted leg is still merely resting and can leave torn risk
   on the book
4. price the unwind as a marketable limit at the current top of book by default
5. leave more aggressive or more conservative slippage control as a later
   strategy-level plug once the gateway module is stable

This default should be revisited only if the runtime later adds true
market-making or other strategies that intentionally prefer passive unwind.

The only meaningful exception is when failure happens before write and the venue
never saw the batch. In that case the batch is aborted locally rather than being
treated as a partial venue outcome.

## OMS Boundary

OMS should continue to own:

- intent acceptance/rejection
- canonical order state
- shard-facing lifecycle emission
- risk reservation and release policy

The gateway should own:

- queueing and batching
- rate-limit admission
- client/session assignment
- transport confidence state
- recovery workflow

### Minimal-churn interpretation

To minimize churn outside transport:

- OMS continues to enqueue the same command variants it does today
- OMS continues to consume the same normalized venue-event stream it does today
- batching, dispatch planning, retry, recovery, and session assignment remain
  transport-internal concerns
- any new gateway-only state should terminate at the gateway boundary rather than
  creating new OMS command categories

## Runtime Invariants

These should be treated as hard design constraints.

1. order-key sequencing is preserved regardless of which REST client executes the
   request
2. grouped dispatch requests are admitted atomically against the write limiter
3. post-write-unknown submit enters recovery without losing lineage or
   `client_order_id`
4. recovery never requires OMS to guess venue state from absence of a response
7. private WS truth is auxiliary and should not be the sole driver of canonical
   lifecycle resolution
8. private WS does not terminate post-write-unknown recovery by itself; REST
   lookup/adoption still runs to completion
9. partial batch success defaults to unwind-first, not continue-submitting-first
10. HTTP 429 always aborts the offending request and flushes the hot order queue
11. shutdown must either drain or explicitly transfer in-flight requests into
   recovery/uncertain handling

## Suggested Implementation Order

1. define gateway types around `DispatchRequest`, `DispatchItem`, transport
   confidence state, and transaction cost
2. define the rate limiter and scheduling classes before touching session code
3. teach the planner to derive batch create and batch cancel requests from
   existing command lineage
4. wire a reserved recovery lane
5. only then replace blocking session mechanics with a true async session state
   machine

## Implementation Guardrails

1. write new gateway types first and adapt old code to them; do not let current
   async prototype types become the new permanent model
2. make the REST client/session executor a dumb codec/session component, not a
   policy engine
3. keep recovery and uncertainty escalation decisions in the coordinator/truth
   mux layer
4. keep batching decisions above the session layer
5. avoid introducing OMS-visible types until the transport-internal model proves
   insufficient

## Open Questions

1. should grouped submits wait briefly for all expected legs before planning a
   batch, or should OMS/gateway emit an explicit group boundary marker?
2. what exact timeout moves a request from `dispatched_pre_write` to recovery or
   uncertain handling?
3. what precise OMS event vocabulary should represent transport-unknown versus
   order-uncertain?
4. should rate-limit admission be time-based only, or also consider stale-signal
   decay when deciding whether to defer a grouped request?
