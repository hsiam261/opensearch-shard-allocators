#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"
OS_URL="http://localhost:9200"

NO_TEARDOWN=false
for arg in "$@"; do
  case "$arg" in
    --no-teardown) NO_TEARDOWN=true ;;
  esac
done

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

PASS_COUNT=0
FAIL_COUNT=0

log()  { echo -e "${YELLOW}>>> $1${NC}"; }
info() { echo -e "${CYAN}    $1${NC}"; }
pass() { echo -e "${GREEN}  PASS: $1${NC}"; PASS_COUNT=$((PASS_COUNT + 1)); }
fail() { echo -e "${RED}  FAIL: $1${NC}"; FAIL_COUNT=$((FAIL_COUNT + 1)); }

ceil_div() { echo $(( ($1 + $2 - 1) / $2 )); }

for cmd in curl jq docker; do
  if ! command -v "$cmd" &>/dev/null; then
    echo "Error: $cmd is required but not installed"
    exit 1
  fi
done

###############################################################################
# Helpers
###############################################################################

wait_green() {
  local timeout=${1:-120}
  local i
  for i in $(seq 1 "$timeout"); do
    local status
    status=$(curl -sf "$OS_URL/_cluster/health" 2>/dev/null | jq -r '.status' 2>/dev/null || echo "unreachable")
    if [ "$status" = "green" ]; then
      return 0
    fi
    sleep 1
  done
  echo "WARNING: cluster not green after ${timeout}s (status: $status)"
  return 1
}

wait_no_relocating() {
  local timeout=${1:-120}
  local i
  for i in $(seq 1 "$timeout"); do
    local relocating
    relocating=$(curl -sf "$OS_URL/_cluster/health" 2>/dev/null | jq -r '.relocating_shards' 2>/dev/null || echo "?")
    if [ "$relocating" = "0" ]; then
      return 0
    fi
    sleep 1
  done
  echo "WARNING: still relocating after ${timeout}s"
  return 1
}

print_distribution() {
  local pattern=$1
  local label=$2
  echo "    $label:"
  curl -s "$OS_URL/_cat/shards/${pattern}?h=node" | sort | uniq -c | sort -rn | sed 's/^/      /'
}

check_ds_balance() {
  local pattern=$1
  local label=$2

  local node_count
  node_count=$(curl -s "$OS_URL/_cat/nodes?h=name" | wc -l)

  local total_shards
  total_shards=$(curl -s "$OS_URL/_cat/shards/${pattern}?h=node" | grep -c . || true)

  if [ "$total_shards" -eq 0 ]; then
    fail "$label — no shards found"
    return 0
  fi

  local unassigned
  unassigned=$(curl -s "$OS_URL/_cat/shards/${pattern}?h=state" | grep -c UNASSIGNED || true)

  if [ "$unassigned" -gt 0 ]; then
    fail "$label — $unassigned UNASSIGNED shards"
    return 0
  fi

  local max_per_node
  max_per_node=$(curl -s "$OS_URL/_cat/shards/${pattern}?h=node" | sort | uniq -c | awk '{print $1}' | sort -rn | head -1)

  local cap
  cap=$(ceil_div "$total_shards" "$node_count")

  if [ "$max_per_node" -le "$cap" ]; then
    pass "$label — $total_shards shards, max/node=$max_per_node, cap=$cap"
  else
    fail "$label — $total_shards shards, max/node=$max_per_node exceeds cap=$cap"
  fi
  print_distribution "$pattern" "distribution"
  return 0
}

get_shard_node() {
  local index=$1
  local prirep=$2
  curl -s "$OS_URL/_cat/shards/${index}?format=json" \
    | jq -r ".[] | select(.prirep==\"$prirep\" and .state==\"STARTED\") | .node"
}

move_shard() {
  local index=$1 shard=$2 from=$3 to=$4
  info "Moving $index [$shard] from $from to $to"
  local result
  result=$(curl -s -X POST "$OS_URL/_cluster/reroute" \
    -H 'Content-Type: application/json' -d "{
    \"commands\": [{
      \"move\": {
        \"index\": \"$index\",
        \"shard\": $shard,
        \"from_node\": \"$from\",
        \"to_node\": \"$to\"
      }
    }]
  }")
  if echo "$result" | jq -e '.error' &>/dev/null; then
    echo "    ERROR: $(echo "$result" | jq -r '.error.reason // .error.type')"
  fi
  wait_no_relocating
}

create_ds_template() {
  local name=$1
  curl -s -X PUT "$OS_URL/_index_template/${name}-template" \
    -H 'Content-Type: application/json' -d "{
    \"index_patterns\": [\"$name\"],
    \"data_stream\": {},
    \"template\": {
      \"settings\": { \"number_of_shards\": 1, \"number_of_replicas\": 1 }
    }
  }" > /dev/null
}

create_datastream() {
  local name=$1
  curl -s -X POST "$OS_URL/${name}/_doc" \
    -H 'Content-Type: application/json' -d "{
    \"@timestamp\": \"2024-01-01T00:00:00\",
    \"message\": \"init\"
  }" > /dev/null
}

rollover_ds() {
  local name=$1
  curl -s -X POST "$OS_URL/${name}/_rollover" > /dev/null
}

delete_datastream() {
  local name=$1
  curl -s -X DELETE "$OS_URL/_data_stream/$name" > /dev/null 2>&1 || true
  curl -s -X DELETE "$OS_URL/_index_template/${name}-template" > /dev/null 2>&1 || true
}

###############################################################################
# Build plugin
###############################################################################

if [ ! -f "$PROJECT_DIR/build/distributions/datastream-allocator-1.0.0.zip" ]; then
  log "Building plugin..."
  (cd "$PROJECT_DIR" && bash build.sh)
fi

###############################################################################
# Start cluster
###############################################################################

log "Starting 3-node OpenSearch cluster..."
docker compose -f "$COMPOSE_FILE" up -d --build

if [ "$NO_TEARDOWN" = false ]; then
  cleanup() {
    log "Tearing down cluster..."
    docker compose -f "$COMPOSE_FILE" down -v
  }
  trap cleanup EXIT
else
  log "Teardown disabled (--no-teardown). Cluster will remain running."
fi

log "Waiting for cluster to be green..."
wait_green 180

info "Cluster is green. Node list:"
curl -s "$OS_URL/_cat/nodes?v&h=name,node.role" | sed 's/^/    /'
echo ""

###############################################################################
# TEST 1: Basic Shard Placement with Rolling Rollovers
###############################################################################

test_1() {
  log "TEST 1: Basic Shard Placement"
  echo ""

  create_ds_template "logs"
  create_ds_template "metrics"
  create_datastream "logs"
  create_datastream "metrics"
  sleep 3
  wait_green

  info "Initial state (1 backing index each, 2 shards each):"
  check_ds_balance ".ds-logs-*" "logs initial"
  check_ds_balance ".ds-metrics-*" "metrics initial"
  echo ""

  for i in $(seq 1 5); do
    rollover_ds "logs"
    sleep 3
    wait_green
    check_ds_balance ".ds-logs-*" "logs after rollover $i"
  done
  echo ""

  for i in $(seq 1 5); do
    rollover_ds "metrics"
    sleep 3
    wait_green
    check_ds_balance ".ds-metrics-*" "metrics after rollover $i"
  done
  echo ""

  info "Final state — both datastreams at 6 backing indices (12 shards each):"
  check_ds_balance ".ds-logs-*" "logs final"
  check_ds_balance ".ds-metrics-*" "metrics final"
  echo ""
}

###############################################################################
# TEST 2: Recovery from Imbalanced State
###############################################################################

test_2() {
  log "TEST 2: Recovery from Imbalanced State"
  echo ""

  info "Disabling rebalancing..."
  curl -s -X PUT "$OS_URL/_cluster/settings" \
    -H 'Content-Type: application/json' -d '{
    "persistent": { "cluster.routing.rebalance.enable": "none" }
  }' > /dev/null

  create_ds_template "events"
  create_datastream "events"

  for i in $(seq 1 5); do
    rollover_ds "events"
    sleep 2
  done
  sleep 3
  wait_green

  info "Initial placement (rebalancing disabled, 6 backing indices, 12 shards):"
  print_distribution ".ds-events-*" "events before force-move"
  echo ""

  info "Force-moving all primaries → node1, all replicas → node2..."
  local indices
  indices=$(curl -s "$OS_URL/_cat/shards/.ds-events-*?h=index" | sort -u)

  for index in $indices; do
    local primary_node replica_node

    primary_node=$(get_shard_node "$index" "p")
    replica_node=$(get_shard_node "$index" "r")

    if [ "$primary_node" != "node1" ]; then
      if [ "$primary_node" = "node2" ]; then
        replica_target="node3"
      else
        replica_target="node2"
      fi

      if [ "$replica_node" != "$replica_target" ]; then
        move_shard "$index" 0 "$replica_node" "$replica_target"
      fi

      move_shard "$index" 0 "$primary_node" "node1"
    fi

    replica_node=$(get_shard_node "$index" "r")
    if [ "$replica_node" != "node2" ]; then
      move_shard "$index" 0 "$replica_node" "node2"
    fi
  done

  echo ""
  info "After force-move (expecting node1=6, node2=6, node3=0):"
  print_distribution ".ds-events-*" "events"

  local node3_count
  node3_count=$(curl -s "$OS_URL/_cat/shards/.ds-events-*?h=node" | grep -c "node3" || true)
  if [ "$node3_count" -eq 0 ]; then
    pass "force-move — node3 has 0 shards"
  else
    fail "force-move — node3 has $node3_count shards (expected 0)"
  fi
  echo ""

  info "Re-enabling rebalancing..."
  curl -s -X PUT "$OS_URL/_cluster/settings" \
    -H 'Content-Type: application/json' -d '{
    "persistent": { "cluster.routing.rebalance.enable": "all" }
  }' > /dev/null

  info "Rolling over events and watching shard distribution..."
  echo ""

  sleep 5

  for i in $(seq 1 6); do
    rollover_ds "events"
    sleep 5
    wait_green
    wait_no_relocating

    local n1 n2 n3
    n1=$(curl -s "$OS_URL/_cat/shards/.ds-events-*?h=node" | grep -c "node1" || true)
    n2=$(curl -s "$OS_URL/_cat/shards/.ds-events-*?h=node" | grep -c "node2" || true)
    n3=$(curl -s "$OS_URL/_cat/shards/.ds-events-*?h=node" | grep -c "node3" || true)
    info "After rollover $i: node1=$n1  node2=$n2  node3=$n3"
  done
  echo ""

  check_ds_balance ".ds-events-*" "events final (24 shards across 3 nodes)"
  echo ""

  info "Creating datastream 'audit' to check independent balancing..."
  create_ds_template "audit"
  create_datastream "audit"

  for i in $(seq 1 3); do
    rollover_ds "audit"
    sleep 3
  done
  wait_green

  info "Audit should balance independently despite events imbalance:"
  check_ds_balance ".ds-audit-*" "audit independent balance"
  echo ""
}

###############################################################################
# Run
###############################################################################

test_1

log "Cleaning up test 1..."
delete_datastream "logs"
delete_datastream "metrics"
sleep 5
wait_green
echo ""

test_2

echo ""
echo "========================================"
echo -e "Results: ${GREEN}${PASS_COUNT} passed${NC}, ${RED}${FAIL_COUNT} failed${NC}"
echo "========================================"

if [ "$FAIL_COUNT" -gt 0 ]; then
  exit 1
fi
