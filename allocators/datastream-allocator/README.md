# Datastream-Aware Shard Allocator

A custom OpenSearch `ShardsAllocator` that balances shards by **datastream** instead of by index.

## Problem

The default `BalancedShardsAllocator` balances at the index level. With datastreams where each backing index has only 1P+1R, per-index balancing is meaningless — every index looks "balanced." But at the datastream level, shards can cluster unevenly, creating hotspots.

An `AllocationDecider` can't solve this — it can only say YES/NO per (shard, node) pair, which causes deadlocks when the cap is tight and `SameShardAllocationDecider` blocks the only eligible node. A custom `ShardsAllocator` avoids this by ranking nodes and falling through to the next best option.

## How It Works

The allocator scores nodes by datastream shard count (lightest first) and walks candidates in order, skipping any blocked by deciders:

- **`allocateUnassigned()`** — places new shards on the node with the fewest shards from the same datastream
- **`moveShards()`** — relocates shards that can no longer remain, targeting the lightest node
- **`balance()`** — proactively rebalances when the delta between heaviest and lightest exceeds a threshold

Non-datastream indices fall back to total-shard-count scoring.

## Activation

```yaml
# opensearch.yml
cluster.routing.allocation.type: datastream_balanced
```

## Settings

| Setting | Default | Description |
|---|---|---|
| `cluster.routing.allocation.datastream_balance.threshold` | `1.0` | Minimum delta (heaviest - lightest) before rebalancing triggers |

## Full Spec

See [SPEC.md](SPEC.md) for the complete design, implementation plan, edge case analysis, and Docker test plan.
