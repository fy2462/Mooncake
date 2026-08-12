# Summary Window Metrics Parity Design

## Scope

Implement the observable contract of C++
`MasterMetricsTest.SummaryUsesWindowRatesAndCumulativeEviction` in the Rust
master. The scope is the production metrics summary boundary: scalar PutStart
window rates, the batch-request heading/rates, and cumulative generic, Memory,
and NoF eviction sections. It does not copy unrelated C++ summary fields.

## Architecture

`metrics.rs` remains the single owner of Prometheus counters. A process-wide
`SummaryState` mutex stores the previous counter snapshot and timestamp. The
public formatter takes a caller-supplied monotonic `Duration` for deterministic
testing, while the production wrapper supplies elapsed time from a lazily
created `Instant` epoch.

Calling the non-updating formatter reads the same saved baseline repeatedly.
Calling the updating formatter atomically formats against the previous baseline
and replaces it with the current counters and timestamp. Counter deltas use
saturating subtraction, and zero elapsed time formats as `0.00`.

## Metrics

Existing PutStart and batch PutStart counters are reused. Add separate
cumulative Memory and NoF eviction attempt/success/key/byte counters alongside
the existing generic counters. Production Memory and NoF eviction cycles record
one attempt per cycle and add successful object/byte totals after the cycle.
The generic counters continue to represent aggregate eviction accounting.

The exact fixture uses fresh-process direct counter increments, matching the
C++ metrics-manager test boundary without constructing eviction candidates.
Production-cycle regression tests separately prove the new counters are wired
to real eviction results.

## Output Contract

The formatter emits these stable sections:

- `Requests (Success/Total per sec): PutStart=<success>/<total>`
- `Batch Requests (per sec, Req=Success/PartialSuccess/Total): PutStart=<...>`
- `Eviction: Success/Attempts=... AllocFail=... keys=... size=...`
- `Mem Eviction: Success/Attempts=... keys=... size=...`
- `NoF Eviction: Success/Attempts=... keys=... size=...`

Rates have two decimal places and no `/s` suffix. Byte formatting follows the
C++ fixture for zero bytes and integral KiB values.

## Testing

An isolated child test establishes a baseline at time zero, increments exactly
the C++ fixture counters, formats at 20 ms, updates the snapshot, and formats an
idle window at 40 ms. It asserts all required headings, cumulative eviction
values, idle zero rates, and forbidden raw-count/`/s` forms. Tests run with one
test thread except the fresh child itself, and no correctness sleep is used.

