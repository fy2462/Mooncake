# K8s HA lifecycle parity design

## Scope

Cover these five C++ rows with uniquely discoverable Rust tests:

- `K8sBasicMasterViewOperations`
- `K8sCanReacquireAfterRelease`
- `K8sContendedLeadershipAndHandover`
- `K8sConnstringValidFormatPassesParsing`
- `K8sConnstringNoSlashDefaultsNamespace`

The boundary is the Kubernetes Lease coordinator. Kubernetes remains rejected
for production HA serving until it has a shared ordered oplog; that separate
serving-policy decision is not part of these coordinator tests.

## Selected approach

Extend `test_k8s_ha_live.rs` with five opt-in live tests. Each test has its own
unique Lease name and uses the existing `MOONCAKE_K8S_E2E` gate, real kube
client, RBAC preflight, and cleanup helper. Reusable fixture setup is factored
without consolidating the five test entry points.

The lifecycle witnesses exercise production `LeaderCoordinator` operations:

- basic: empty/stale-safe initial read, acquire, exact view, renew, stable
  short timeout, release, and release-induced view change;
- reacquire: the same coordinator acquires/renews/releases twice with distinct
  leader addresses;
- contention: A acquires and renews, B is contended and observes A's exact view
  version, A releases, B observes change and takes over;
- valid `namespace/lease`: construction is followed by a real read/acquire and
  release so lazy Rust construction cannot create a false positive;
- name-only: production parsing must default to namespace `default`, followed
  by a real acquire/read/release against `Api<Lease>::namespaced(..., "default")`.

## Failure and skip policy

Without `MOONCAKE_K8S_E2E`, every test emits an explicit stderr skip and returns.
Once enabled, unusable kube configuration, denied RBAC, API failures, unexpected
views, and cleanup failures are hard test failures. Cleanup is attempted after
normal completion; unique names prevent cross-test interference.

## Verification

Compile and run the five exact test names without the live gate to prove they
are discoverable and their skip path is bounded. If the environment has no live
K8s cluster, disclose that the five tests were opt-in skipped; do not claim a
local live pass. Run the master test target and parity validator, then record
each C++ row against its exact Rust test and repair commit.
