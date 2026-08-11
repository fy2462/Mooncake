# K8s HA Client Spec Availability Parity Design

## Goal

Classify `HABackendAvailabilityTest.ClientSpecParsingMatchesK8sBuildFlag`
against the actual Rust product boundary and retain one exact Rust serving
policy witness.

## C++ Oracle

For `k8s://default/master`, a C++ build with `STORE_USE_K8S_LEASE` returns a
K8s backend spec whose connstring is exactly `default/master`; a build without
that capability returns `UNAVAILABLE_IN_CURRENT_MODE`.

## Rust Boundary

Rust does not expose the C++ client-side `k8s://` URI parser or compile K8s
election support behind an equivalent Store feature. It can construct a K8s
coordinator spec from Master CLI fields, but production serving deliberately
rejects K8s because it has no shared ordered oplog. That is a different stage
and reason from the C++ client parser's build-availability rejection.

Retain one test in `test_main_config.rs` that sets backend type `k8s` and the
exact connstring payload `default/master`, then:

1. calls the production `build_ha_spec` path;
2. asserts K8s type and exact connstring preservation;
3. calls `validate_ha_backend_for_serving` on that same spec;
4. asserts `HaError::UnavailableInCurrentMode` and the shared-oplog reason.

The manifest row is `not-applicable` with the `cpp-build-or-abi` category,
matching the adjacent K8s availability row. The test is supporting evidence
for Rust's distinct policy, not a claim that Rust parses the full C++ URI.

## Scope and Verification

No production change is expected. Run the exact test, the complete
`test_main_config` binary, scoped formatting, parity validators and validator
contracts. Stage only the new test because the checkout contains unrelated
accumulated work.
