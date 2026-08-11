# K8s HA Client Spec Availability Parity Design

## Goal

Close
`HABackendAvailabilityTest.ClientSpecParsingMatchesK8sBuildFlag` with one
exact Rust configuration witness.

## C++ Oracle

For `k8s://default/master`, a C++ build with `STORE_USE_K8S_LEASE` returns a
K8s backend spec whose connstring is exactly `default/master`; a build without
that capability returns `UNAVAILABLE_IN_CURRENT_MODE`.

## Rust Boundary

Rust does not compile K8s election support behind an equivalent Store feature.
It can construct a K8s coordinator spec, but production serving deliberately
rejects K8s because it has no shared ordered oplog. Therefore the portable
Rust outcome corresponds to the C++ unavailable branch while still preserving
the parsed spec fields.

Add one test in `test_main_config.rs` that sets backend type `k8s` and the exact
connstring `default/master`, then:

1. calls the production `build_ha_spec` path;
2. asserts K8s type and exact connstring preservation;
3. calls `validate_ha_backend_for_serving` on that same spec;
4. asserts `HaError::UnavailableInCurrentMode` and the shared-oplog reason.

## Scope and Verification

No production change is expected. Run the exact test, the complete
`test_main_config` binary, scoped formatting, parity validators and validator
contracts. Stage only the new test because the checkout contains unrelated
accumulated work.
