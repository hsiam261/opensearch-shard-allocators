# Datastream-Aware Shard Allocator — Spec & Implementation Plan

## Problem

The default `BalancedShardsAllocator` balances shards at the **index** level. In a logs cluster using datastreams, each backing index has only 1 primary + 1 replica. The allocator sees each backing index in isolation and considers it "balanced" — but at the datastream level, shards can cluster unevenly:

```
Datastream "logs" — 6 backing indices, 1P+1R each = 12 shards, 3 nodes

What the default allocator sees (per-index):
  .ds-logs-000001: 1P on A, 1R on B  ✓ balanced
  .ds-logs-000002: 1P on A, 1R on C  ✓ balanced
  .ds-logs-000003: 1P on A, 1R on B  ✓ balanced
  ... etc

What actually happened (datastream level):
  Node A: 6 shards (all primaries!)   ← hotspot
  Node B: 4 shards
  Node C: 2 shards                    ← underused
```

The per-index weight component is useless here — every index has exactly 1 shard per type, so it's always "balanced" at that level.

## Why Not an AllocationDecider?

A decider can only say YES or NO per (shard, node) pair. It cannot rank nodes or fall through to the next best option. This causes deadlocks:

```
3 nodes, 4 backing indices (8 shards), cap = ceil(8/3) = 3

  Node A: 3 datastream shards (at cap)
  Node B: 3 datastream shards (at cap)
  Node C: 2 datastream shards (includes primary of ds-000004[0])

Placing replica of ds-000004[0]:
  Datastream decider:  Node A = NO (at cap)   Node B = NO (at cap)   Node C = YES
  SameShardDecider:                                                    Node C = NO

  → ALL nodes blocked → shard stays UNASSIGNED forever
```

A custom `ShardsAllocator` doesn't have this problem. It tries nodes in order (lightest first) and naturally skips to the next node when one is blocked:

```
Try Node C (2 shards, lightest) → SameShardDecider: NO → skip
Try Node A (3 shards, next)     → all deciders: YES → assign here ✓
```

The shard is placed. Slightly uneven (4-3-2), but the rebalance pass evens it out.

## Goal

Replace the default `BalancedShardsAllocator` with a custom `ShardsAllocator` that:

1. Groups shards by **datastream** instead of by index
2. Scores nodes by datastream shard count (prefer the node with the fewest shards from the same datastream)
3. Falls through to the next-best node when deciders block the best one
4. Leaves non-datastream indices to be handled by total-shard-count balancing
5. Handles all three operations: `allocateUnassigned`, `moveShards`, `balance`

## Design

### Weight Function

```
For datastream-backed indices:
  weight(node, datastream) = node.datastreamShardCount - avgDatastreamShardsPerNode

For non-datastream indices:
  weight(node) = node.totalShardCount - avgShardsPerNode
```

A node with more datastream shards than average gets a higher weight (less desirable). The allocator picks the node with the lowest weight, checks all `AllocationDeciders`, and falls through to the next if blocked.

No per-index component needed — every backing index has 1 shard, so per-index balancing is meaningless.

### Architecture

```
DatastreamShardsAllocator implements ShardsAllocator
│
├── allocate(RoutingAllocation)
│   │
│   ├── allocateUnassigned()
│   │     For each UNASSIGNED shard:
│   │       1. Resolve datastream from metadata
│   │       2. Score nodes by datastream shard count (ascending)
│   │       3. Walk sorted list:
│   │          - Check AllocationDeciders.canAllocate()
│   │          - First YES → assign, break
│   │          - All NO → shard stays UNASSIGNED
│   │
│   ├── moveShards()
│   │     For each STARTED shard on every node:
│   │       1. Check AllocationDeciders.canRemain()
│   │       2. If NO → find lightest eligible node by datastream count
│   │       3. Relocate there
│   │
│   └── balance()
│         For each datastream:
│           1. Compute weight per node
│           2. Sort nodes by weight
│           3. If delta (heaviest - lightest) > threshold:
│              - Pick shard from heaviest node
│              - Check canRebalance() + canAllocate() on lightest
│              - If YES → relocate
│              - If NO → try next lightest
│              - Repeat until delta ≤ threshold
│         For non-datastream shards:
│           Same logic but scored by total shard count
│
├── decideShardAllocation(ShardRouting, RoutingAllocation)
│     Provide explanation for _cluster/allocation/explain API
│
└── setRerouteService(RerouteService)
      Store handle for triggering future reroutes
```

### Resolving Datastream Membership

```java
private String resolveDatastream(String indexName, Metadata metadata) {
    IndexAbstraction abs = metadata.getIndicesLookup().get(indexName);
    if (abs == null) return null;
    IndexAbstraction.DataStream parent = abs.getParentDataStream();
    if (parent == null) return null;
    return parent.getName();
}
```

- `metadata.getIndicesLookup()` returns `SortedMap<String, IndexAbstraction>` — always populated, never null
- `IndexAbstraction.getParentDataStream()` returns null for non-datastream indices
- Both are `@PublicApi(since="1.0.0")` — stable across versions

### Counting Datastream Shards on a Node

```java
private int countDatastreamShards(RoutingNode node, String datastreamName, Metadata metadata) {
    int count = 0;
    for (ShardRouting shard : node) {
        String ds = resolveDatastream(shard.getIndexName(), metadata);
        if (datastreamName.equals(ds)) {
            count++;
        }
    }
    return count;
}
```

This counts STARTED, INITIALIZING, and RELOCATING shards — all states that occupy a node. This prevents cascading moves (a node receiving a shard already looks heavier).

### Settings

```java
// Threshold for rebalancing (same concept as BalancedShardsAllocator)
public static final Setting<Float> THRESHOLD_SETTING = Setting.floatSetting(
    "cluster.routing.allocation.datastream_balance.threshold",
    1.0f,           // default: same as built-in
    0.0f,           // min
    Setting.Property.NodeScope,
    Setting.Property.Dynamic
);
```

### Activation

In `opensearch.yml`:
```yaml
cluster.routing.allocation.type: datastream_balanced
```

### Plugin Class

```java
public class DatastreamAllocatorPlugin extends Plugin implements ClusterPlugin {

    @Override
    public Map<String, Supplier<ShardsAllocator>> getShardsAllocators(
            Settings settings, ClusterSettings clusterSettings) {
        return Map.of(
            "datastream_balanced",
            () -> new DatastreamShardsAllocator(settings, clusterSettings)
        );
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            DatastreamShardsAllocator.THRESHOLD_SETTING
        );
    }
}
```

## Implementation

### Core Classes

```
opensearch-datastream-allocator/
├── build.gradle
├── src/
│   ├── main/
│   │   ├── java/com/example/datastreamallocator/
│   │   │   ├── DatastreamAllocatorPlugin.java
│   │   │   ├── DatastreamShardsAllocator.java
│   │   │   └── DatastreamWeightFunction.java
│   │   └── plugin-metadata/
│   │       (no plugin-security.policy needed)
│   └── test/
│       └── java/com/example/datastreamallocator/
│           ├── DatastreamShardsAllocatorTests.java
│           └── DatastreamWeightFunctionTests.java
```

### Phase 1: Skeleton

1. Set up gradle project with `opensearch.opensearchplugin` (or standalone build)
2. Create `DatastreamAllocatorPlugin` with empty `getShardsAllocators()`
3. Create `DatastreamShardsAllocator` implementing `ShardsAllocator` — all methods empty/no-op
4. Create `plugin-descriptor.properties`
5. Build ZIP, verify it installs and OpenSearch starts with `cluster.routing.allocation.type: datastream_balanced`

### Phase 2: allocateUnassigned()

This is the most important operation — it places new shards.

```java
public void allocateUnassigned(RoutingAllocation allocation) {
    Metadata metadata = allocation.metadata();
    RoutingNodes.UnassignedShards unassigned = allocation.routingNodes().unassigned();
    RoutingNodes.UnassignedShards.UnassignedIterator iter = unassigned.iterator();

    while (iter.hasNext()) {
        ShardRouting shard = iter.next();
        String datastream = resolveDatastream(shard.getIndexName(), metadata);

        // Build candidate list sorted by datastream shard count (ascending)
        List<RoutingNode> candidates = new ArrayList<>();
        for (RoutingNode node : allocation.routingNodes()) {
            candidates.add(node);
        }

        if (datastream != null) {
            // Sort by datastream shard count (lightest first)
            candidates.sort(Comparator.comparingInt(
                node -> countDatastreamShards(node, datastream, metadata)
            ));
        } else {
            // Non-datastream: sort by total shard count
            candidates.sort(Comparator.comparingInt(RoutingNode::size));
        }

        // Try each candidate in order
        boolean assigned = false;
        for (RoutingNode node : candidates) {
            Decision decision = allocation.deciders().canAllocate(shard, node, allocation);
            if (decision.type() == Decision.Type.YES) {
                iter.initialize(node.nodeId(), null, -1L, allocation.changes());
                assigned = true;
                break;
            }
        }
        // If no node accepted, shard stays UNASSIGNED (iter moves on)
    }
}
```

**Test:** create a datastream with 6 backing indices on 3 nodes. Verify 4-4-4 distribution.

### Phase 3: moveShards()

Handles shards that can no longer remain on their node (decider says `canRemain = NO`).

```java
public void moveShards(RoutingAllocation allocation) {
    Metadata metadata = allocation.metadata();

    for (RoutingNode node : allocation.routingNodes()) {
        for (ShardRouting shard : node.copyShards()) {
            if (!shard.started()) continue;

            Decision remainDecision = allocation.deciders().canRemain(shard, node, allocation);
            if (remainDecision.type() == Decision.Type.NO) {
                // Must move — find best target
                String datastream = resolveDatastream(shard.getIndexName(), metadata);

                List<RoutingNode> candidates = new ArrayList<>();
                for (RoutingNode target : allocation.routingNodes()) {
                    if (target.nodeId().equals(node.nodeId())) continue;
                    candidates.add(target);
                }

                if (datastream != null) {
                    candidates.sort(Comparator.comparingInt(
                        n -> countDatastreamShards(n, datastream, metadata)
                    ));
                } else {
                    candidates.sort(Comparator.comparingInt(RoutingNode::size));
                }

                for (RoutingNode target : candidates) {
                    Decision allocateDecision = allocation.deciders()
                        .canAllocate(shard, target, allocation);
                    if (allocateDecision.type() == Decision.Type.YES) {
                        allocation.routingNodes().relocateShard(
                            shard, target.nodeId(), -1L, allocation.changes()
                        );
                        break;
                    }
                }
            }
        }
    }
}
```

**Test:** add a filter rule excluding a node → verify shards move off it, distributed by datastream count.

### Phase 4: balance()

Proactive rebalancing — move shards from heavy to light nodes.

```java
public void balance(RoutingAllocation allocation) {
    Metadata metadata = allocation.metadata();

    // Collect all datastreams
    Set<String> datastreams = new HashSet<>();
    for (RoutingNode node : allocation.routingNodes()) {
        for (ShardRouting shard : node) {
            String ds = resolveDatastream(shard.getIndexName(), metadata);
            if (ds != null) datastreams.add(ds);
        }
    }

    // Rebalance each datastream independently
    for (String datastream : datastreams) {
        rebalanceDatastream(datastream, allocation, metadata);
    }

    // Rebalance non-datastream shards by total count
    rebalanceNonDatastream(allocation, metadata);
}

private void rebalanceDatastream(String datastream, RoutingAllocation allocation,
                                  Metadata metadata) {
    List<RoutingNode> nodes = new ArrayList<>();
    for (RoutingNode node : allocation.routingNodes()) {
        nodes.add(node);
    }

    boolean changed = true;
    while (changed) {
        changed = false;

        // Sort by datastream shard count
        nodes.sort(Comparator.comparingInt(
            n -> countDatastreamShards(n, datastream, metadata)
        ));

        RoutingNode lightest = nodes.get(0);
        RoutingNode heaviest = nodes.get(nodes.size() - 1);

        int lightCount = countDatastreamShards(lightest, datastream, metadata);
        int heavyCount = countDatastreamShards(heaviest, datastream, metadata);
        float delta = heavyCount - lightCount;

        if (delta <= threshold) break;

        // Find a datastream shard on heaviest that can move to lightest
        for (ShardRouting shard : heaviest.copyShards()) {
            if (!shard.started()) continue;
            String ds = resolveDatastream(shard.getIndexName(), metadata);
            if (!datastream.equals(ds)) continue;

            Decision rebalanceDecision = allocation.deciders()
                .canRebalance(shard, allocation);
            if (rebalanceDecision.type() != Decision.Type.YES) continue;

            Decision allocateDecision = allocation.deciders()
                .canAllocate(shard, lightest, allocation);
            if (allocateDecision.type() == Decision.Type.YES) {
                allocation.routingNodes().relocateShard(
                    shard, lightest.nodeId(), -1L, allocation.changes()
                );
                changed = true;
                break;
            }
            // If lightest blocked, try next-lightest
            for (int i = 1; i < nodes.size() - 1; i++) {
                RoutingNode alt = nodes.get(i);
                int altCount = countDatastreamShards(alt, datastream, metadata);
                if (heavyCount - altCount <= threshold) break;

                Decision altDecision = allocation.deciders()
                    .canAllocate(shard, alt, allocation);
                if (altDecision.type() == Decision.Type.YES) {
                    allocation.routingNodes().relocateShard(
                        shard, alt.nodeId(), -1L, allocation.changes()
                    );
                    changed = true;
                    break;
                }
            }
            if (changed) break;
        }
    }
}
```

**Test:** add a 4th node → verify shards rebalance from 4-4-4 to 3-3-3-3.

### Phase 5: decideShardAllocation()

For the `_cluster/allocation/explain` API:

```java
@Override
public ShardAllocationDecision decideShardAllocation(ShardRouting shard,
                                                      RoutingAllocation allocation) {
    // Build node decisions with datastream shard counts
    // Return AllocateUnassignedDecision or MoveDecision with explanations
}
```

Not critical for functionality, but makes debugging much easier.

### Phase 6: Edge Cases

1. **Non-datastream indices**: fall back to total-shard-count scoring
2. **Single-node cluster**: everything goes on that node (deciders handle the rest)
3. **Mixed cluster**: some indices in datastreams, some not — scored independently
4. **Empty datastream name**: shouldn't happen (metadata always has it), but guard with null check
5. **Concurrent relocations**: shards counted on both source and target — prevents cascading
6. **THROTTLE decisions**: respect throttle — don't skip to next node, just stop trying this shard for now

### Phase 7: Performance Optimization

The naive implementation calls `resolveDatastream()` and `countDatastreamShards()` for every (shard, node) pair. For large clusters, pre-compute:

```java
// Build once per allocate() call
Map<String, Map<String, Integer>> datastreamCountPerNode;
// datastreamCountPerNode.get("logs").get("node1") = 4

// Also cache
Map<String, String> indexToDatastream;
// indexToDatastream.get(".ds-logs-000001") = "logs"
```

---

## Docker Test Plan

### Setup

#### Dockerfile

```dockerfile
FROM opensearchproject/opensearch:2.19.0

COPY opensearch-datastream-allocator-1.0.0.zip /tmp/
RUN /usr/share/opensearch/bin/opensearch-plugin install --batch \
    file:///tmp/opensearch-datastream-allocator-1.0.0.zip && \
    rm /tmp/opensearch-datastream-allocator-1.0.0.zip

RUN echo "cluster.routing.allocation.type: datastream_balanced" >> \
    /usr/share/opensearch/config/opensearch.yml
```

#### docker-compose.yml

```yaml
version: '3'
services:
  opensearch-node1:
    build: .
    container_name: opensearch-node1
    environment:
      - cluster.name=test-cluster
      - node.name=node1
      - discovery.seed_hosts=opensearch-node1,opensearch-node2,opensearch-node3
      - cluster.initial_cluster_manager_nodes=node1,node2,node3
      - bootstrap.memory_lock=true
      - "OPENSEARCH_JAVA_OPTS=-Xms512m -Xmx512m"
      - DISABLE_SECURITY_PLUGIN=true
    ulimits:
      memlock: { soft: -1, hard: -1 }
    ports:
      - 9200:9200

  opensearch-node2:
    build: .
    container_name: opensearch-node2
    environment:
      - cluster.name=test-cluster
      - node.name=node2
      - discovery.seed_hosts=opensearch-node1,opensearch-node2,opensearch-node3
      - cluster.initial_cluster_manager_nodes=node1,node2,node3
      - bootstrap.memory_lock=true
      - "OPENSEARCH_JAVA_OPTS=-Xms512m -Xmx512m"
      - DISABLE_SECURITY_PLUGIN=true

  opensearch-node3:
    build: .
    container_name: opensearch-node3
    environment:
      - cluster.name=test-cluster
      - node.name=node3
      - discovery.seed_hosts=opensearch-node1,opensearch-node2,opensearch-node3
      - cluster.initial_cluster_manager_nodes=node1,node2,node3
      - bootstrap.memory_lock=true
      - "OPENSEARCH_JAVA_OPTS=-Xms512m -Xmx512m"
      - DISABLE_SECURITY_PLUGIN=true

  # Node 4 is defined but not started initially (used in Test 3)
  opensearch-node4:
    build: .
    container_name: opensearch-node4
    environment:
      - cluster.name=test-cluster
      - node.name=node4
      - discovery.seed_hosts=opensearch-node1,opensearch-node2,opensearch-node3,opensearch-node4
      - cluster.initial_cluster_manager_nodes=node1,node2,node3
      - bootstrap.memory_lock=true
      - "OPENSEARCH_JAVA_OPTS=-Xms512m -Xmx512m"
      - DISABLE_SECURITY_PLUGIN=true
    profiles:
      - node4  # only starts when explicitly requested
```

---

### Test 1: Even Distribution on Fresh Datastream

**Verifies:** `allocateUnassigned()` distributes shards evenly by datastream.

```bash
echo "=== Test 1: Even Distribution ==="

# Create index template
curl -s -X PUT "localhost:9200/_index_template/logs-template" \
  -H 'Content-Type: application/json' -d '{
  "index_patterns": ["logs"],
  "data_stream": {},
  "template": {
    "settings": { "number_of_shards": 1, "number_of_replicas": 1 }
  }
}'

# Create datastream + rollover to 6 backing indices
curl -s -X POST "localhost:9200/logs/_doc" \
  -H 'Content-Type: application/json' -d '{
  "@timestamp": "2024-01-01T00:00:00", "message": "init"
}'
for i in $(seq 1 5); do
  curl -s -X POST "localhost:9200/logs/_rollover" > /dev/null
  sleep 2
done

# Wait for green
curl -s "localhost:9200/_cluster/health?wait_for_status=green&timeout=30s" > /dev/null

# Verify: 12 shards, max 4 per node
COUNTS=$(curl -s "localhost:9200/_cat/shards/.ds-logs-*?h=node" | sort | uniq -c | awk '{print $1}')
MAX=$(echo "$COUNTS" | sort -rn | head -1)

if [ "$MAX" -le 4 ]; then
  echo "PASS: max per node = $MAX (expected <= 4)"
else
  echo "FAIL: max per node = $MAX (expected <= 4)"
fi

echo "Distribution:"
curl -s "localhost:9200/_cat/shards/.ds-logs-*?h=node" | sort | uniq -c
```

**Pass:** No node has more than `ceil(12/3) = 4`. All 12 shards assigned.

---

### Test 2: Non-Datastream Indices Unaffected

**Verifies:** allocator uses total-shard-count scoring for regular indices.

```bash
echo "=== Test 2: Non-Datastream Passthrough ==="

curl -s -X PUT "localhost:9200/regular-index" \
  -H 'Content-Type: application/json' -d '{
  "settings": { "number_of_shards": 3, "number_of_replicas": 1 }
}'

curl -s "localhost:9200/_cluster/health/regular-index?wait_for_status=green&timeout=30s" > /dev/null

UNASSIGNED=$(curl -s "localhost:9200/_cat/shards/regular-index?h=state" | grep -c UNASSIGNED || true)
TOTAL=$(curl -s "localhost:9200/_cat/shards/regular-index?h=state" | wc -l)

if [ "$UNASSIGNED" -eq 0 ] && [ "$TOTAL" -eq 6 ]; then
  echo "PASS: all 6 shards assigned, none stuck"
else
  echo "FAIL: $UNASSIGNED unassigned out of $TOTAL"
fi

curl -s "localhost:9200/_cat/shards/regular-index?v&h=index,shard,prirep,node"

curl -s -X DELETE "localhost:9200/regular-index" > /dev/null
```

**Pass:** All 6 shards assigned. Distribution is reasonable (2 per node).

---

### Test 3: New Node Triggers Rebalance

**Verifies:** `balance()` redistributes when a node joins.

```bash
echo "=== Test 3: Node Join Rebalance ==="

# Confirm starting state: 4-4-4
echo "Before node4:"
curl -s "localhost:9200/_cat/shards/.ds-logs-*?h=node" | sort | uniq -c

# Start node4
docker compose --profile node4 up -d opensearch-node4
sleep 45

# Wait for green with 4 nodes
curl -s "localhost:9200/_cluster/health?wait_for_status=green&timeout=60s" > /dev/null

echo "After node4 joined:"
COUNTS=$(curl -s "localhost:9200/_cat/shards/.ds-logs-*?h=node" | sort | uniq -c | awk '{print $1}')
MAX=$(echo "$COUNTS" | sort -rn | head -1)
NODE_COUNT=$(echo "$COUNTS" | wc -l)

# cap = ceil(12/4) = 3
if [ "$MAX" -le 3 ] && [ "$NODE_COUNT" -eq 4 ]; then
  echo "PASS: max per node = $MAX, shards on $NODE_COUNT nodes (expected <= 3, 4 nodes)"
else
  echo "FAIL: max=$MAX, nodes=$NODE_COUNT"
fi

curl -s "localhost:9200/_cat/shards/.ds-logs-*?h=node" | sort | uniq -c
```

**Pass:** 3-3-3-3 distribution. No node has more than 3.

---

### Test 4: Node Death and Recovery

**Verifies:** shards redistribute after node loss, then rebalance on return.

```bash
echo "=== Test 4: Node Death and Recovery ==="

# Kill node3
docker stop opensearch-node3
sleep 30

# Check: 2 surviving nodes, cap = ceil(12/2) = 6
# (with 4 nodes it was 3 nodes + node4; adjust if node4 is still up)
curl -s "localhost:9200/_cluster/health?wait_for_status=yellow&timeout=60s" > /dev/null

echo "After node3 death:"
UNASSIGNED=$(curl -s "localhost:9200/_cat/shards/.ds-logs-*?h=state" | grep -c UNASSIGNED || true)
curl -s "localhost:9200/_cat/shards/.ds-logs-*?h=node" | sort | uniq -c

if [ "$UNASSIGNED" -eq 0 ]; then
  echo "PASS: no unassigned shards"
else
  echo "FAIL: $UNASSIGNED unassigned shards"
fi

# Bring node3 back
docker start opensearch-node3
sleep 45

curl -s "localhost:9200/_cluster/health?wait_for_status=green&timeout=60s" > /dev/null

echo "After node3 return:"
curl -s "localhost:9200/_cat/shards/.ds-logs-*?h=node" | sort | uniq -c

MAX=$(curl -s "localhost:9200/_cat/shards/.ds-logs-*?h=node" | sort | uniq -c | awk '{print $1}' | sort -rn | head -1)
NODE_COUNT=$(curl -s "localhost:9200/_cat/nodes?h=name" | wc -l)
EXPECTED_CAP=$(python3 -c "import math; print(math.ceil(12/$NODE_COUNT))")

if [ "$MAX" -le "$EXPECTED_CAP" ]; then
  echo "PASS: rebalanced, max=$MAX (cap=$EXPECTED_CAP)"
else
  echo "FAIL: max=$MAX exceeds cap=$EXPECTED_CAP"
fi
```

**Pass:** After death, all shards assigned. After return, rebalanced within cap.

---

### Test 5: Rollover Adjusts Dynamically

**Verifies:** new backing indices are distributed respecting the updated cap.

```bash
echo "=== Test 5: Rollover ==="

# Starting: 6 backing indices, 12 shards
# Rollover 3 more times → 9 backing indices, 18 shards
for i in $(seq 1 3); do
  curl -s -X POST "localhost:9200/logs/_rollover" > /dev/null
  sleep 2
done

sleep 15
curl -s "localhost:9200/_cluster/health?wait_for_status=green&timeout=30s" > /dev/null

NODE_COUNT=$(curl -s "localhost:9200/_cat/nodes?h=name" | wc -l)
EXPECTED_CAP=$(python3 -c "import math; print(math.ceil(18/$NODE_COUNT))")
MAX=$(curl -s "localhost:9200/_cat/shards/.ds-logs-*?h=node" | sort | uniq -c | awk '{print $1}' | sort -rn | head -1)

echo "18 shards across $NODE_COUNT nodes, cap=$EXPECTED_CAP, max=$MAX"
if [ "$MAX" -le "$EXPECTED_CAP" ]; then
  echo "PASS"
else
  echo "FAIL"
fi
```

**Pass:** Cap adjusts to `ceil(18/N)`. No node exceeds it.

---

### Test 6: Multiple Datastreams Are Independent

**Verifies:** each datastream is balanced independently.

```bash
echo "=== Test 6: Multiple Datastreams ==="

# Create second datastream
curl -s -X PUT "localhost:9200/_index_template/metrics-template" \
  -H 'Content-Type: application/json' -d '{
  "index_patterns": ["metrics"],
  "data_stream": {},
  "template": {
    "settings": { "number_of_shards": 1, "number_of_replicas": 1 }
  }
}'

curl -s -X POST "localhost:9200/metrics/_doc" \
  -H 'Content-Type: application/json' -d '{
  "@timestamp": "2024-01-01T00:00:00", "value": 42
}'
for i in $(seq 1 2); do
  curl -s -X POST "localhost:9200/metrics/_rollover" > /dev/null
  sleep 2
done

sleep 10
curl -s "localhost:9200/_cluster/health?wait_for_status=green&timeout=30s" > /dev/null

echo "logs distribution:"
curl -s "localhost:9200/_cat/shards/.ds-logs-*?h=node" | sort | uniq -c

echo "metrics distribution:"
curl -s "localhost:9200/_cat/shards/.ds-metrics-*?h=node" | sort | uniq -c

# Verify metrics independently balanced (6 shards)
NODE_COUNT=$(curl -s "localhost:9200/_cat/nodes?h=name" | wc -l)
METRICS_CAP=$(python3 -c "import math; print(math.ceil(6/$NODE_COUNT))")
METRICS_MAX=$(curl -s "localhost:9200/_cat/shards/.ds-metrics-*?h=node" | sort | uniq -c | awk '{print $1}' | sort -rn | head -1)

if [ "$METRICS_MAX" -le "$METRICS_CAP" ]; then
  echo "PASS: metrics max=$METRICS_MAX (cap=$METRICS_CAP)"
else
  echo "FAIL: metrics max=$METRICS_MAX (cap=$METRICS_CAP)"
fi
```

**Pass:** Each datastream balanced independently. Counts don't interfere.

---

### Test 7: No Deadlock with SameShardAllocationDecider

**Verifies:** primary and replica of the same backing index always get placed, even at tight caps.

```bash
echo "=== Test 7: No Deadlock ==="

# Create a new datastream with only 1 backing index on a 3-node cluster
# 2 shards (1P + 1R), cap = ceil(2/3) = 1
# Both must be placed on different nodes

curl -s -X PUT "localhost:9200/_index_template/tiny-template" \
  -H 'Content-Type: application/json' -d '{
  "index_patterns": ["tiny"],
  "data_stream": {},
  "template": {
    "settings": { "number_of_shards": 1, "number_of_replicas": 1 }
  }
}'

curl -s -X POST "localhost:9200/tiny/_doc" \
  -H 'Content-Type: application/json' -d '{
  "@timestamp": "2024-01-01T00:00:00", "message": "test"
}'

sleep 10

UNASSIGNED=$(curl -s "localhost:9200/_cat/shards/.ds-tiny-*?h=state" | grep -c UNASSIGNED || true)
curl -s "localhost:9200/_cat/shards/.ds-tiny-*?v&h=index,shard,prirep,state,node"

if [ "$UNASSIGNED" -eq 0 ]; then
  echo "PASS: no deadlock, both shards assigned"
else
  echo "FAIL: $UNASSIGNED shards unassigned (deadlock!)"
fi

curl -s -X DELETE "localhost:9200/_data_stream/tiny" > /dev/null
curl -s -X DELETE "localhost:9200/_index_template/tiny-template" > /dev/null
```

**Pass:** Both shards assigned on different nodes. No UNASSIGNED.

This is the key test that proves the allocator approach works where a decider would deadlock. At cap=1, only one node is "lightest." If primary is there, the allocator skips to the next node for the replica.

---

### Test 8: Explain API

**Verifies:** `_cluster/allocation/explain` shows datastream-aware reasoning.

```bash
echo "=== Test 8: Explain API ==="

curl -s -X GET "localhost:9200/_cluster/allocation/explain?pretty" \
  -H 'Content-Type: application/json' -d "{
  \"index\": \"$(curl -s 'localhost:9200/_cat/shards/.ds-logs-*?h=index' | head -1)\",
  \"shard\": 0,
  \"primary\": true
}"

# EXPECTED: explanation includes datastream shard counts per node
```

**Pass:** Output includes datastream-aware node scoring.

---

### Test 9: moveShards on Filter Change

**Verifies:** `moveShards()` relocates shards respecting datastream balance when a constraint changes.

```bash
echo "=== Test 9: Filter Change ==="

echo "Before filter:"
curl -s "localhost:9200/_cat/shards/.ds-logs-*?h=node" | sort | uniq -c

# Exclude node1 from logs indices
curl -s -X PUT "localhost:9200/.ds-logs-*/_settings" \
  -H 'Content-Type: application/json' -d '{
  "index.routing.allocation.exclude._name": "node1"
}'

sleep 20

echo "After excluding node1:"
NODE1_COUNT=$(curl -s "localhost:9200/_cat/shards/.ds-logs-*?h=node" | grep -c "node1" || true)

if [ "$NODE1_COUNT" -eq 0 ]; then
  echo "PASS: no shards on node1"
else
  echo "FAIL: $NODE1_COUNT shards still on node1"
fi

curl -s "localhost:9200/_cat/shards/.ds-logs-*?h=node" | sort | uniq -c

# Remove the filter
curl -s -X PUT "localhost:9200/.ds-logs-*/_settings" \
  -H 'Content-Type: application/json' -d '{
  "index.routing.allocation.exclude._name": null
}'

sleep 20
echo "After removing filter:"
curl -s "localhost:9200/_cat/shards/.ds-logs-*?h=node" | sort | uniq -c
```

**Pass:** After filter, node1 has 0 shards. After removing, rebalances back.

---

### Test 10: Rolling Restart

**Verifies:** cluster survives rolling restarts with correct rebalancing.

```bash
echo "=== Test 10: Rolling Restart ==="

for node in opensearch-node1 opensearch-node2 opensearch-node3; do
  echo "Restarting $node..."
  docker restart $node
  sleep 45

  STATUS=$(curl -s "localhost:9200/_cluster/health" | grep -o '"status":"[a-z]*"' | cut -d'"' -f4)
  echo "Cluster status: $STATUS"

  curl -s "localhost:9200/_cat/shards/.ds-logs-*?h=node" | sort | uniq -c
  echo "---"
done

UNASSIGNED=$(curl -s "localhost:9200/_cat/shards/.ds-logs-*?h=state" | grep -c UNASSIGNED || true)
if [ "$UNASSIGNED" -eq 0 ]; then
  echo "PASS: no unassigned after full rolling restart"
else
  echo "FAIL: $UNASSIGNED unassigned"
fi
```

**Pass:** After full rolling restart, all shards assigned and balanced.

---

### Automated Test Runner

```bash
#!/bin/bash
# run-tests.sh
set -euo pipefail

PASS=0
FAIL=0

pass() { echo "PASS: $1"; PASS=$((PASS + 1)); }
fail() { echo "FAIL: $1"; FAIL=$((FAIL + 1)); }

wait_green() {
  for i in $(seq 1 60); do
    s=$(curl -s "localhost:9200/_cluster/health?wait_for_status=green&timeout=5s" \
        | grep -o '"status":"[a-z]*"' | cut -d'"' -f4)
    [ "$s" = "green" ] && return 0
    sleep 5
  done
  return 1
}

check_max() {
  local pattern=$1 expected=$2 label=$3
  local max unassigned
  max=$(curl -s "localhost:9200/_cat/shards/${pattern}?h=node" \
        | sort | uniq -c | awk '{print $1}' | sort -rn | head -1)
  unassigned=$(curl -s "localhost:9200/_cat/shards/${pattern}?h=state" \
               | grep -c UNASSIGNED || true)
  if [ "$unassigned" -gt 0 ]; then
    fail "$label — $unassigned UNASSIGNED"
  elif [ "$max" -le "$expected" ]; then
    pass "$label — max=$max (limit=$expected)"
  else
    fail "$label — max=$max exceeds limit=$expected"
  fi
}

echo "=== Datastream Allocator Test Suite ==="
echo "Waiting for cluster..."
wait_green

# Run tests 1-10 here, calling pass/fail/check_max as needed
# ...

echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
exit $FAIL
```

---

## Version Compatibility

### What can break across OpenSearch versions

| Component | Risk | Detection |
|---|---|---|
| `ShardsAllocator` interface | Medium — new methods can be added | Compile error |
| `RoutingAllocation` API | Medium — constructor params can change | Compile error |
| `RoutingNodes` mutation methods | Medium — signatures may change | Compile error |
| `AllocationDeciders` API | Low — stable | Compile error |
| `Metadata.getIndicesLookup()` | Low — `@PublicApi` | Compile error |
| `IndexAbstraction.getParentDataStream()` | Low — `@PublicApi` | Compile error |
| `ShardRouting` fields | Low — but new states possible | Runtime bugs |
| Serialization format | N/A — allocator doesn't serialize | N/A |

### Upgrade process

1. Update `opensearch.version` in `plugin-descriptor.properties`
2. Recompile against new OpenSearch version
3. Fix any `ShardsAllocator` interface changes (most likely: new methods with defaults)
4. Run unit tests
5. Run Docker integration tests against the new version's image
6. Publish new plugin ZIP

### Semver range

```properties
dependencies={opensearch: "~2.19.0"}
```

Supports 2.19.x without rebuilding. Rebuild needed for 2.20.0+.
