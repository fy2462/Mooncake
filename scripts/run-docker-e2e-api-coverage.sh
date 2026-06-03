#!/usr/bin/env bash
# Mooncake multi-node API coverage E2E test (Docker)
# Validates: BatchUpsertEnd (PutEndEntry), MountSegment (te_endpoint+protocol),
# ReMountSegment (base_addrs+te_endpoints+protocols), upsert, batch ops, health_check
set -e

MOONCAKE_ROOT="/home/fy2462/workspace/Mooncake"
NETWORK="mooncake-net"
IMG="mooncake-py314t"
RUN_ID="api-cov-$(date +%s)"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'

echo -e "${YELLOW}=== Mooncake Multi-Node API Coverage E2E (Python 3.14t, Docker) ===${NC}"
echo "RUN_ID: $RUN_ID"

# --- Verify prerequisites ---
if ! command -v docker &>/dev/null; then
    echo -e "${RED}docker not found${NC}"; exit 1
fi

WHEEL=$(ls "$MOONCAKE_ROOT"/rust-repo/target/wheels/mooncake_store_rust-0.1.0-cp314-cp314t-*.whl 2>/dev/null | head -1)
if [ -z "$WHEEL" ]; then
    echo -e "${RED}cp314 wheel not found. Build it first:${NC}"
    echo "  cd rust-repo/python && maturin build --release"
    exit 1
fi
echo "Wheel: $(basename $WHEEL)"

if [ ! -f "$MOONCAKE_ROOT/rust-repo/target/debug/mooncake-master" ]; then
    echo -e "${RED}Rust master binary not found. Build it first:${NC}"
    echo "  cargo build --package mooncake-master"
    exit 1
fi

# --- Build/check Docker image ---
if ! docker image inspect "$IMG" &>/dev/null; then
    echo -e "${YELLOW}Building Docker image...${NC}"
    docker build \
        -t "$IMG" \
        -f "$MOONCAKE_ROOT/scripts/Containerfile.py314t" \
        "$MOONCAKE_ROOT" \
        2>&1 | tail -5
fi
echo -e "${GREEN}[OK] Image ready${NC}"

# --- Cleanup previous containers ---
echo "Cleaning up..."
docker rm -f mc-client mc-storage mc-master mc-etcd 2>/dev/null || true
docker network rm "$NETWORK" 2>/dev/null || true

# --- Create network ---
echo "Creating Docker network..."
docker network create --subnet 10.89.0.0/24 "$NETWORK"

# --- Start etcd ---
echo -e "${YELLOW}Starting etcd...${NC}"
docker run -d --rm --name mc-etcd --network "$NETWORK" --hostname etcd \
    -v "$MOONCAKE_ROOT:$MOONCAKE_ROOT" \
    "$IMG" \
    etcd --data-dir /tmp/etcd-data \
    --listen-client-urls http://0.0.0.0:2379 \
    --advertise-client-urls http://10.89.0.10:2379

sleep 3
docker exec mc-etcd etcdctl endpoint health 2>&1
ETCD_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' mc-etcd)
echo -e "${GREEN}[OK] etcd healthy (IP: $ETCD_IP)${NC}"

# --- Start Master ---
echo -e "${YELLOW}Starting Mooncake Master...${NC}"
docker run -d --rm --name mc-master --network "$NETWORK" --hostname master \
    -v "$MOONCAKE_ROOT:$MOONCAKE_ROOT" \
    "$IMG" \
    /home/fy2462/workspace/Mooncake/rust-repo/target/debug/mooncake-master \
    --etcd-endpoints "${ETCD_IP}:2379" \
    --rpc-port 50051

sleep 3
MASTER_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' mc-master)
echo -e "${GREEN}[OK] Master started (IP: $MASTER_IP)${NC}"

# --- Start Storage Node ---
echo -e "${YELLOW}Starting Storage Node (tests MountSegment w/ te_endpoint+protocol)...${NC}"
docker run -d --rm --name mc-storage --network "$NETWORK" --hostname storage \
    -v "$MOONCAKE_ROOT:$MOONCAKE_ROOT" \
    -e ROLE=storage \
    -e ETCD_ENDPOINTS="${ETCD_IP}:2379" \
    -e MASTER_ADDR="${MASTER_IP}:50051" \
    -e RUN_ID="$RUN_ID" \
    "$IMG"

sleep 5
STORAGE_LOG=$(docker logs mc-storage 2>&1 | tail -15)
echo "$STORAGE_LOG" | while read line; do echo "  [storage] $line"; done
if echo "$STORAGE_LOG" | grep -q "Node ready"; then
    echo -e "${GREEN}[OK] Storage node ready (MountSegment w/ te_endpoint+protocol verified)${NC}"
else
    echo -e "${YELLOW}[WARN] Storage node may not be fully ready${NC}"
fi

# --- Run Client Tests ---
echo ""
echo -e "${YELLOW}=== Running API Coverage Tests ===${NC}"
echo "  (BatchUpsertEnd, upsert, health_check/ReMountSegment, batch ops)"

docker run --rm --name mc-client --network "$NETWORK" --hostname client \
    -v "$MOONCAKE_ROOT:$MOONCAKE_ROOT" \
    -e ROLE=client \
    -e ETCD_ENDPOINTS="${ETCD_IP}:2379" \
    -e MASTER_ADDR="${MASTER_IP}:50051" \
    -e RUN_ID="$RUN_ID" \
    "$IMG" \
    /bin/bash -c "\
        uv pip install --force-reinstall ${MOONCAKE_ROOT}/rust-repo/target/wheels/mooncake_store_rust-0.1.0-cp314-cp314t-manylinux_2_39_x86_64.whl 2>&1 && \
        echo '[docker] wheel installed' && \
        python --version && \
        python /home/fy2462/workspace/Mooncake/scripts/debug_multi_node_api_coverage.py \
    " 2>&1 | while IFS= read -r line; do
        # Filter: show only client-level log lines and pass/fail status
        if echo "$line" | grep -qE '\[client\]|PASS|FAIL|Results|🎯|passed|failed|\[docker\]'; then
            echo "  $line"
        elif echo "$line" | grep -qiE 'traceback|error|crash|panic'; then
            echo -e "  ${RED}$line${NC}"
        fi
    done

CLIENT_EXIT=${PIPESTATUS[0]}

# Show summary
echo ""
docker logs mc-client 2>&1 | grep -E 'PASS|FAIL|Results|🎯|passed|failed' || true

# --- Cleanup ---
echo ""
echo "Cleaning up..."
docker rm -f mc-client mc-storage mc-master mc-etcd 2>/dev/null || true
docker network rm "$NETWORK" 2>/dev/null || true

echo ""
if [ $CLIENT_EXIT -eq 0 ]; then
    echo -e "${GREEN}🎯 API coverage E2E tests PASSED${NC}"
else
    echo -e "${RED}❌ API coverage E2E tests FAILED (exit: $CLIENT_EXIT)${NC}"
fi
exit $CLIENT_EXIT
