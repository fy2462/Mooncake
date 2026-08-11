# K8s HA lifecycle parity implementation plan

1. Add shared live setup/cleanup utilities that retain the existing environment
   gate, rustls setup, RBAC check, unique Lease names, and real `Api<Lease>`.
2. Add five exact Rust test functions matching the five C++ rows. Keep the
   existing broad backoff/watch E2E unchanged as regression coverage.
3. Compile and run the exact tests, then run the complete
   `test_k8s_ha_live` target and relevant master tests. Record whether the live
   gate executed or explicitly skipped.
4. Commit only the K8s test file, then update the five manifest rows and append
   five remediation-log entries with the code SHA and honest verification.
5. Validate JSON and parity references, commit only the narrow documentation
   changes, and request independent oracle review.
