## Outcome

<!-- What capability or invariant changes, and why is it needed? -->

## Ownership and data flow

<!-- Which thread owns the new state? Which queues/messages cross boundaries? -->

## Failure and recovery behavior

<!-- What fails closed? What is retried, invalidated, reconciled, or operator-visible? -->

## Trading and research boundary

<!-- State whether this changes live authorization, limits, execution behavior, or only research/mechanics. -->

## Validation

<!-- Include exact build/test commands and bounded live/replay evidence. Separate exercised paths from untested paths. -->

## Checklist

- [ ] C++ CI configure/build and CTest pass
- [ ] Python operator/config tests pass
- [ ] New state has one explicit owner
- [ ] Every new SPSC queue has exactly one producer and one consumer
- [ ] Backpressure and terminal ownership paths are covered
- [ ] Trading remains disabled until explicit operator authorization
- [ ] Config, operator, and telemetry surfaces are documented
- [ ] Known gaps and paths not exercised are stated explicitly

## Related

<!-- Link issue(s), ticket(s), or discussion(s) -->
