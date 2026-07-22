#!/usr/bin/env bash
set -euo pipefail

suite_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)
artifact_root=/home/fy2462/workspace/tmp/mooncake/rdma-multinode
mkdir -p "$artifact_root"
test_tmp=$(mktemp -d "$artifact_root/test-common.XXXXXX")
trap 'rm -rf -- "$test_tmp"' EXIT

# shellcheck source=../lib/common.sh
source "$suite_dir/lib/common.sh"

test "$RDMA_ARTIFACT_ROOT" = "$artifact_root"
owned_name mc-rdma-te-a
! owned_name unrelated-container
! owned_name ""
! owned_name /
! owned_name '~'

printf 'status=FAIL\nprotocol=rdma\n' >"$test_tmp/te.result"
! run_if_te_passed "$test_tmp/te.result" touch "$test_tmp/store-ran"
test ! -e "$test_tmp/store-ran"

printf 'status=PASS\nprotocol=tcp\n' >"$test_tmp/te.result"
! run_if_te_passed "$test_tmp/te.result" touch "$test_tmp/store-ran"
test ! -e "$test_tmp/store-ran"

printf 'status=PASS\nprotocol=rdma\n' >"$test_tmp/te.result"
run_if_te_passed "$test_tmp/te.result" touch "$test_tmp/store-ran"
test -e "$test_tmp/store-ran"

evidence_file="$test_tmp/evidence.tsv"
record topology independent-rxe
grep -Fx $'topology\tindependent-rxe' "$evidence_file"

printf 'ready\n' >"$test_tmp/docker.log"
fake_bin="$test_tmp/bin"
mkdir -p "$fake_bin"
cat >"$fake_bin/docker" <<EOF
#!/usr/bin/env bash
if [[ "\$1 \$2" == "logs mc-rdma-test" ]]; then
    cat "$test_tmp/docker.log"
fi
EOF
chmod +x "$fake_bin/docker"
PATH="$fake_bin:$PATH" wait_for_log mc-rdma-test ready 1
