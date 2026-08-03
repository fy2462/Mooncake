# Snapshot child catalog descriptor parity implementation plan

1. Create an isolated worktree from the committed design and plan.
2. Add three stable integration tests in
   `mooncake-store-master/tests/test_catalog_snapshot.rs` using real local
   object and embedded catalog stores.
3. Run each test with one deliberately wrong primary assertion and retain the
   three failing transcripts; restore immediately after every mutation.
4. Run all three exact tests for ten rounds and require 30 selected passes,
   zero failures, and ten three-test summaries.
5. Run catalog, Master library, complete package, and all-target gates.
6. Commit the tests, update exactly three parity rows and remediation records
   with the test commit SHA, then run validators and repository hygiene gates.
7. Request independent review, fix every Important-or-higher finding, fast-forward
   merge to `rust_repo_main`, and rerun the complete package on merged main.
