#!/usr/bin/env bash
# Mooncake multi-node E2E tenant isolation test using docker
set -e

MOONCAKE_ROOT="/home/fy2462/workspace/Mooncake"
NETWORK="mooncake-tenant-net"
IMG="mooncake-test"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
echo -e "${YELLOW}=== Mooncake Multi-Tenant E2E Test (Docker) ===${NC}"

echo "Cleaning up previous containers..."
docker rm -f mc-client mc-storage mc-master mc-etcd 2>/dev/null || true
docker network rm $NETWORK 2>/dev/null || true

echo "Creating network..."
docker network create --subnet 10.89.1.0/24 $NETWORK

echo "Starting etcd..."
docker run -d --rm --name mc-etcd --network $NETWORK --hostname etcd \
    --ip 10.89.1.10 \
    -v $MOONCAKE_ROOT:$MOONCAKE_ROOT \
    $IMG \
    etcd --data-dir /tmp/etcd-data \
    --listen-client-urls http://0.0.0.0:2379 \
    --advertise-client-urls http://10.89.1.10:2379

sleep 3
docker exec mc-etcd etcdctl endpoint health 2>&1
echo -e "${GREEN}[OK] etcd healthy${NC}"

echo "Starting Mooncake Master..."
docker run -d --rm --name mc-master --network $NETWORK --hostname master \
    -v $MOONCAKE_ROOT:$MOONCAKE_ROOT \
    $IMG \
    /home/fy2462/workspace/Mooncake/rust-repo/target/debug/mooncake-master \
    --rpc-address 0.0.0.0 --rpc-port 50051

sleep 2
MASTER_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' mc-master)
echo -e "${GREEN}[OK] Master started (IP: $MASTER_IP)${NC}"

echo "Starting Storage Node..."
docker run -d --rm --name mc-storage --network $NETWORK --hostname storage \
    -v $MOONCAKE_ROOT:$MOONCAKE_ROOT \
    -e ROLE=storage \
    -e ETCD_ENDPOINTS=10.89.1.10:2379 \
    -e MASTER_ADDR=${MASTER_IP}:50051 \
    $IMG \
    python /home/fy2462/workspace/Mooncake/scripts/mooncake-e2e-tenant-test.py

sleep 6
STORAGE_LOG=$(docker logs mc-storage 2>&1 | tail -5)
echo "  Storage: $(echo "$STORAGE_LOG" | grep -o 'Node ready\|ERROR' || echo 'waiting...')"

echo -e "${YELLOW}=== Running Tenant E2E Tests ===${NC}"

# Override CMD to run tenant test
CLIENT_OUTPUT=$(mktemp)
docker run --rm --name mc-client --network $NETWORK --hostname client \
    -v $MOONCAKE_ROOT:$MOONCAKE_ROOT \
    -e ROLE=client \
    -e ETCD_ENDPOINTS=10.89.1.10:2379 \
    -e MASTER_ADDR=${MASTER_IP}:50051 \
    --entrypoint python \
    $IMG \
    /home/fy2462/workspace/Mooncake/scripts/mooncake-e2e-tenant-test.py 2>&1 | tee $CLIENT_OUTPUT

CLIENT_EXIT=$?

echo ""
echo "Cleaning up..."
docker rm -f mc-client mc-storage mc-master mc-etcd 2>/dev/null || true
docker network rm $NETWORK 2>/dev/null || true

if [ $CLIENT_EXIT -eq 0 ]; then
    echo -e "${GREEN}🎯 Multi-tenant E2E tests PASSED${NC}"
else
    echo -e "${RED}❌ Multi-tenant E2E tests FAILED (exit: $CLIENT_EXIT)${NC}"
fi
exit $CLIENT_EXIT
