# LocalDisk snapshot round-trip parity implementation plan

1. Add four exact tests and shared source/publish/restore/recovery helpers inside
   the service snapshot test module. First compile them against the absent
   persisted-client field/helper to record red.
2. Extend `LocalDiskSegmentEntry` with a non-authoritative persisted client
   identity. Populate it on every mount/classic creation and preserve it across
   fresh restore and second capture.
3. Preserve dormant LocalDisk policy/queue fields and replica metadata during
   restore, while keeping active session, recovery state, capacity, and
   `handle_valid` fenced.
4. Complete Memory remount and LocalDisk inventory recovery through production
   RPCs in each witness, then assert exact public replica descriptors and
   first/second snapshot equivalence.
5. Run focused, regression, formatting, and full master verification. Commit
   only the production/test files, then stage narrow HEAD-relative manifest and
   ledger patches for the four rows and request independent review.
