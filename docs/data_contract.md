# Pipeline Data Contract

This is the runtime handoff contract for the ingest and routing pipeline:

```text
Websocket text -> RawFrame -> ExtractedFields -> RoutedFrame -> RouterFrame -> NormalizedEvent -> BookStore
```

## 1) Ingest (`RawFrame`)

Type: `trading::ingest::RawFrame`

- `recv_timestamp`: local monotonic receive timestamp.
- `source`: exchange/source label (`"kalshi"`, `"polymarket"`, ...).
- `payload`: full websocket JSON text.
- `market_ticker`: optional fast-path value (usually empty at ingest).
- `seq_id`: optional fast-path sequence id (usually empty at ingest).

Producer:
- `FramePoolMessageSink` writes this once per websocket message.

Consumer:
- `Router` (via `LivePipeline::pump_ingest`).

## 2) Minimal Extract (`ExtractedFields`)

Type: `trading::decode::ExtractedFields`

- `status`: `kOk`, `kMissingField`, `kUnsupportedMessage`.
- `market_ticker`: required for routing when `status == kOk`.
- `seq_id`: optional sequence id.
- `recv_timestamp`: propagated from `RawFrame`.
- `source`: propagated from `RawFrame`.

Producer:
- exchange-specific extractor (e.g. `decode::exchanges::kalshi::extract`).

Consumer:
- `Router`.

## 3) Router Output (`router::RoutedFrame`)

Type: `trading::router::RoutedFrame`

- `exchange`: normalized exchange id enum.
- `market_ticker`: routing key.
- `sequence_id`: optional sequence id.
- `recv_ns`: normalized receive time (`TimestampNs`).
- `raw_payload`: owned payload string forwarded to shard parser.

Producer:
- `Router::route`.

Consumer:
- `ShardedEventDispatch` queues one `RoutedEvent` per target shard.

## 4) Parser Input (`internal::RouterFrame`)

Type: `trading::internal::RouterFrame`

- Same semantic fields as `RoutedFrame`.
- `raw_payload_view`: non-owning view used by exchange parser.

Producer:
- `RoutedEventParser` adapter converts `RoutedEvent` to `RouterFrame`.

Consumer:
- exchange parser (`parsers::exchanges::*::Parser`).

Lifetime rule:
- `raw_payload_view` is only valid during parse call.

## 5) Parser Output (`internal::NormalizedEvent`)

Type: `trading::internal::NormalizedEvent`

- `type`: `Snapshot`, `Delta`, `Trade`, etc.
- `meta`: normalized metadata (`exchange`, timestamps, optional sequence).
- `market_ticker`: canonical market key.
- `raw_sequence_id`: optional source sequence id.
- `data`: event payload variant (`SnapshotData`, `DeltaData`, `TradeData`).
- `raw_payload`: optional retained source payload (parser-dependent).

Producer:
- exchange parser.

Consumer:
- `BookStore::apply`.

Price unit note:
- For Kalshi parser output, `PriceTicks` is normalized to `1e-4` dollars (sub-cent safe).
- Legacy integer cent fields are upscaled (`95` cents -> `9500`).
- Dollar fixed-point fields are preserved at up to 4 decimals (`0.5790` -> `5790`).

## 6) Book Apply (`BookStore`)

Type: `trading::shards::BookStore` / `BookState`

- Snapshot replaces book levels.
- Delta updates one side/price level (`delta_qty_lots` add/remove semantics).
- Trade updates `last_trade`.
- Sequence policy: strictly increasing per book when sequence exists.
  - Contiguous increments are not required (supports connection-global sequence feeds).

## Threading + Ownership Invariants

- `FramePoolMessageSink` is producer for `SpscFrameQueue`.
- `LivePipeline::pump_ingest` is consumer for `SpscFrameQueue`.
- Each shard queue (`SpscRoutedEventQueue`) is:
  - single producer (`Router` thread)
  - single consumer (one shard thread)
- `BookStore` is shard-local (no shared writes across shard threads).
