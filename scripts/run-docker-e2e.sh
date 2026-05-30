#!/usr/bin/env bash
# Mooncake multi-node e2e test orchestration using docker
set -e

MOONCAKE_ROOT="/home/fy2462/workspace/Mooncake"
NETWORK="mooncake-net"
IMG="mooncake-test"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
echo -e "${YELLOW}=== Mooncake Multi-Node E2E Test (Docker) ===${NC}"

# --- Build ---
echo "Building image..."
docker build -t $IMG -f "$MOONCAKE_ROOT/scripts/Containerfile" "$MOONCAKE_ROOT" 2>&1 | tail -5

# --- Cleanup ---
echo "Cleaning up previous containers..."
docker rm -f mc-client mc-storage mc-master mc-etcd 2>/dev/null || true
docker network rm $NETWORK 2>/dev/null || true

# --- Network ---
echo "Creating network..."
docker network create --subnet 10.89.0.0/24 $NETWORK

# --- etcd ---
echo "Starting etcd..."
docker run -d --rm --name mc-etcd --network $NETWORK --hostname etcd \
    -v $MOONCAKE_ROOT:$MOONCAKE_ROOT \
    $IMG \
    etcd --data-dir /tmp/etcd-data \
    --listen-client-urls http://0.0.0.0:2379 \
    --advertise-client-urls http://10.89.0.10:2379

sleep 3
docker exec mc-etcd etcdctl endpoint health 2>&1
echo -e "${GREEN}[OK] etcd healthy${NC}"
ETCD_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' mc-etcd)
echo "  etcd IP: $ETCD_IP"

# --- Master ---
echo "Starting Mooncake Master..."
docker run -d --rm --name mc-master --network $NETWORK --hostname master \
    -v $MOONCAKE_ROOT:$MOONCAKE_ROOT \
    $IMG \
    /home/fy2462/workspace/Mooncake/rust-repo/target/debug/mooncake-master \
    --rpc-address 0.0.0.0 --rpc-port 50051

sleep 2
MASTER_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' mc-master)
echo -e "${GREEN}[OK] Master started (IP: $MASTER_IP)${NC}"

# --- Quick .so loading check ---
echo "Verifying library loading..."
docker exec mc-master python -c "
import sys
sys.path.insert(0, '$MOONCAKE_ROOT/mooncake-wheel')
sys.path.insert(0, '$MOONCAKE_ROOT/rust-repo/python')
import mooncake.engine; print('[OK] engine.so')
from mooncake_store import MooncakeClient; print('[OK] mooncake_store')
" 2>&1 || { echo -e "${RED}[FAIL] library check${NC}"; exit 1; }
echo -e "${GREEN}[OK] Libraries load correctly${NC}"

# --- Storage Node ---
echo "Starting Storage Node..."
docker run -d --rm --name mc-storage --network $NETWORK --hostname storage \
    -v $MOONCAKE_ROOT:$MOONCAKE_ROOT \
    -e ROLE=storage \
    -e ETCD_ENDPOINTS=${ETCD_IP}:2379 \
    -e MASTER_ADDR=${MASTER_IP}:50051 \
    $IMG

sleep 6
STORAGE_LOG=$(docker logs mc-storage 2>&1 | tail -5)
echo "  Storage log:"
echo "$STORAGE_LOG" | while read line; do echo "    $line"; done
if echo "$STORAGE_LOG" | grep -q "Node ready"; then
    echo -e "${GREEN}[OK] Storage node ready${NC}"
else
    echo -e "${YELLOW}[WARN] Storage node may not be fully ready (see log above)${NC}"
fi

# --- Client Node (runs the tests) ---
echo ""
echo -e "${YELLOW}=== Running Client E2E Tests ===${NC}"
docker run --rm --name mc-client --network $NETWORK --hostname client \
    -v $MOONCAKE_ROOT:$MOONCAKE_ROOT \
    -e ROLE=client \
    -e ETCD_ENDPOINTS=${ETCD_IP}:2379 \
    -e MASTER_ADDR=${MASTER_IP}:50051 \
    $IMG 2>&1

CLIENT_EXIT=$?

# --- Cleanup ---
echo ""
echo "Cleaning up..."
docker rm -f mc-client mc-storage mc-master mc-etcd 2>/dev/null || true
docker network rm $NETWORK 2>/dev/null || true

if [ $CLIENT_EXIT -eq 0 ]; then
    echo -e "${GREEN}🎯 Multi-node e2e tests PASSED${NC}"
else
    echo -e "${RED}❌ Multi-node e2e tests FAILED (exit: $CLIENT_EXIT)${NC}"
fi
exit $CLIENT_EXIT
