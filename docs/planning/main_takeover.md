# Main Takeover Checklist

This checklist tracks the mechanical work required before `epic/evolution`
replaces the current `main` runtime. It does not by itself authorize larger
live trading limits.

## Branch shape

At the start of merge preparation:

```text
branch: epic/evolution
behind main: 0 commits
ahead of main: 56 commits
change size: 223 files, approximately 34.5k additions / 24k deletions
```

Because this is a platform replacement, review should be organized by owner and
invariant rather than presented as one undifferentiated feature diff.

## Required merge gates

- [x] Clean CI-equivalent CMake configure with the vcpkg manifest
- [x] Clean CI-equivalent C++ build
- [x] `predex_tests` passes
- [x] The distributed C++ runtime test target passes
- [x] Dependency-free Python operator/config suite passes
- [x] Current app config example is valid JSON and reflects the live schema
- [x] README names the current binaries and operator workflow
- [x] Architecture, ownership, recovery, OMS, data, and Python docs reflect the
      current implementation
- [x] Live soak evidence distinguishes infrastructure health from unexercised
      execution behavior
- [ ] Run the updated GitHub Actions workflow on the pushed merge-prep commit
- [ ] Triage the existing clang-tidy baseline; fix correctness findings before
      deciding which style diagnostics remain advisory
- [x] Run the sanitizer preset and CTest with local leak detection disabled
- [ ] Run LeakSanitizer on a runner without the local ptrace restriction
- [x] Enforce the validated broad-universe capacity defaults in C++ and Python
- [ ] Perform one final `main...HEAD` provenance/status check immediately before
      opening the PR
- [ ] Review the final PR diff for credentials, run artifacts, generated data,
      and accidental large files

## Current local validation

```bash
VCPKG_ROOT=/home/andrew/vcpkg cmake --preset ci-vcpkg
cmake --build --preset build-ci-vcpkg --parallel 2
ctest --preset test-ci-vcpkg

PYTHONPATH=python/src .venv/bin/python -m unittest \
  python.tests.tooling.test_env \
  python.tests.tooling.test_discovery \
  python.tests.tooling.test_replay
```

Observed result:

```text
C++ build: completed
CTest: distributed runtime test target passed
Python: supported tooling suite passed
```

The sanitizer preset also built successfully. The distributed runtime tests
passed under ASan/UBSan with `ASAN_OPTIONS=detect_leaks=0`. LeakSanitizer itself cannot run
inside the local ptrace-constrained environment and must be exercised on the
GitHub runner.

## Static-analysis debt

The inherited workflow ran all enabled clang-tidy checks as errors. The current
replacement branch reports `128` diagnostics across style, analyzer, and
correctness categories. Examples include:

- possible exception escape from functions declared `noexcept`;
- unchecked optional access;
- dead stores in the Unix command server;
- redundant `std::move` on trivially copyable values;
- cognitive-complexity and easily-swappable-parameter warnings;
- magic-number and redundant-inline style findings;
- analyzer padding advice for the deliberately cache-line-separated SPSC.

The workflow now runs build/tests as required gates and retains clang-tidy as a
visible advisory step. This is a transition state, not a declaration that the
diagnostics are acceptable. Triage order should be:

1. `bugprone-exception-escape`
2. `bugprone-unchecked-optional-access`
3. analyzer dead stores and ownership findings
4. performance diagnostics with hot-path impact
5. mechanical readability/style diagnostics
6. intentional cache-line padding, documented and suppressed at its definition

Once categories 1-4 are clean, promote the agreed check set back to a required
gate and keep a separate explicit cleanup list for remaining style debt.

## Formatting

The manual clang-format workflow currently reports broad violations across the
replacement runtime, including the Unix command server and existing config
headers/tests. Do not run a repository-wide formatter blindly immediately
before review; that would mix large mechanical churn into an already large
architecture diff.

Prefer one of:

- format the new/replaced production runtime as a dedicated commit and rerun
  build/tests;
- make formatting advisory for this takeover, then enforce it on changed files
  after `main` moves to the new baseline.

The choice should be explicit in the PR rather than hidden by disabling the
workflow.

## Capacity-default decision

C++ and Python now default to a `65,536`-slot frame pool and `32,768`-entry
router/shard queues. The validated broad-universe soak observed high-water
marks of `22,228`, `74`, and `6,275` respectively. A Python unit test pins the
generator defaults so they cannot silently shrink away from the runtime
policy. Recovery remains the correctness mechanism when capacity is still
exceeded.

## Non-blocking known runtime gaps

These should be visible in the PR but do not have to block replacing the older
runtime if scope remains explicit:

- automatic public/private websocket reconnect and resubscription;
- strategy candidate and rejection-gate telemetry;
- live evidence for partial multi-leg execution and OMS repair;
- evidence sufficient to increase the current small trading allocation.

The merge can establish the safer architecture without claiming those later
operational milestones are complete.

## Suggested PR review order

1. Composition root, control lifecycle, and operator server
2. Public wire session, frame pool, router, and integrity barriers
3. Shard book-state/recovery semantics and strategy publication
4. Strategy evaluation and intent contract
5. OMS, persistent REST, private feed, portfolio, and repair
6. Configuration/discovery and operator tooling
7. Telemetry, latency, tests, and research isolation
8. Documentation and declared known gaps
