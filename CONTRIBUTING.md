# Contributing

## Repository Notes

This repository is in an active architecture cleanup phase.

The current supported C++ surface is:
- `predex_websocket`
- `predex_core_pipeline`
- `predex_app`
- `trader_app`
- `replay_app`
- `parser_regression_test`, `kalshi_rest_adapter_boundary_test` (test executables)

The private-WS transport headers under `cpp/include/predex/oms/transport/` (`private_ws_worker.hpp`, `kalshi_private_ws_adapter.hpp`) are explicit scaffolds for a future wiring step and are not constructed by `App` today.

If you touch runtime architecture, queue topology, or ownership semantics, update the docs alongside the code:
- `README.md`
- `docs/architecture.md`
- `docs/ownership_invariants.md`
- `docs/data_contract.md`

## Branch Strategy

1. Never commit directly to `main`.
2. Create short-lived branches from `main`:
   - `feat/<name>` for features
   - `fix/<name>` for bug fixes
   - `chore/<name>` for maintenance
3. Keep PRs focused and small enough to review quickly.

## Pull Request Rules

1. Open a PR early (draft PR is fine).
2. Keep the PR description updated.
3. Link related issues in the PR description.
4. Ensure CI passes before requesting review.
5. If this is a solo repo, approval is optional.
6. If collaborators are added, require at least one approval before merge.
7. Resolve all review comments before merge.

## Merge Rules

1. Use **Squash and merge** unless there is a specific reason not to.
2. Delete branch after merge.
3. Never bypass required checks.

## Commit Messages

Use clear commit prefixes when possible:

- `feat: ...`
- `fix: ...`
- `chore: ...`
- `docs: ...`
- `test: ...`

## Tooling Expectations

1. C++:
   - Build with CMake
   - Keep code formatted via `.clang-format`
   - Keep static analysis clean via `.clang-tidy` for the active build graph
   - Prefer changing currently-supported targets over reviving stale scaffolding opportunistically
2. Python:
   - Python tooling is planned and may be added incrementally
   - When Python project files are introduced, keep lint and tests green
