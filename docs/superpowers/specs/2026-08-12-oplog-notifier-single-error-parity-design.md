# OpLog Notifier Single-Error Parity Design

## Scope

Cover `OpLogReplicatorTest.InjectError_NotifiesCallback` with executable Rust
evidence and align the HotStandby notifier callback boundary with C++. A single
error delivered through a started change notifier must emit
`StandbyEvent::WatchBroken`, moving a watching standby into reconnecting.

This change does not alter the etcd notifier's internal retry or reconnect
policy. The notifier may still use its existing consecutive-error threshold to
decide whether its own worker exits; this design only defines what happens once
that notifier invokes the callback supplied by HotStandby.

## Considered Approaches

1. Immediately process `WatchBroken` in the HotStandby `on_error` callback.
   This is selected because it matches the C++ `OpLogReplicator` callback
   boundary exactly and leaves backend-specific retry behavior encapsulated in
   each notifier.
2. Reduce the etcd notifier's internal threshold from ten errors to one. This
   would conflate transport retry policy with service state notification and
   would change reconnect behavior beyond the mapped C++ oracle.
3. Add only a state-machine unit test that directly submits `WatchBroken`.
   Existing tests already prove that transition; such a test would not prove
   that a notifier error actually reaches it.

## Production Behavior

The callback passed to `OpLogChangeNotifier::start` will process
`StandbyEvent::WatchBroken` whenever the service is not already recovering or
failed. From `Watching`, the existing state machine commits `Reconnecting` and
marks the watch unhealthy. Repeated or late callbacks remain safe because the
existing transition table handles reconnecting states and the callback ignores
terminal recovery/failure states.

The surrounding notifier worker keeps its current startup, health polling,
shutdown, and backend retry logic. No etcd constants, polling intervals, or
error counters change.

## Test Witness

Add a focused HotStandby test using the existing mock notifier/store boundary.
The fixture starts a real `HotStandbyService`, waits until it reaches
`Watching`, injects exactly one notifier error, and waits for the production
callback to run. It then asserts:

- state is `Reconnecting`;
- `is_connected` is false;
- the watch is not healthy and the service is not ready for promotion;
- the service can be stopped cleanly.

The test must fail against the current implementation because one callback only
increments the state machine's consecutive-error count and leaves the service
in `Watching`.

## Parity Accounting and Verification

After the witness passes, update only the
`OpLogReplicatorTest.InjectError_NotifiesCallback` manifest row from `missing`
to `covered`, naming the exact Rust test and callback path. Do not claim that
the Rust etcd transport retry loop is identical to C++; the covered behavior is
the notifier-to-service callback boundary.

Run the exact new test first, then adjacent HotStandby notifier/state-machine
tests with one test thread. Finish with Rust formatting, JSON validation, the
parity validator, cached-diff checks, and independent Critical/Important review
before committing the implementation batch.
