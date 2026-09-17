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

## Building

The plugin builds inside Docker (no local Gradle/JDK required). Pass the target OpenSearch version as the first argument (defaults to `2.19.0`):

```bash
bash build.sh <opensearch-version>
```

For example:

```bash
bash build.sh 2.19.0
```

This produces `build/distributions/datastream-allocator-1.0.0-opensearch-2.19.0.zip`, which can be installed with:

```bash
opensearch-plugin install --batch file:///path/to/datastream-allocator-1.0.0-opensearch-2.19.0.zip
```

Supported OpenSearch versions: 2.13–2.19.

## Testing

Integration tests run against a 3-node OpenSearch cluster in Docker with the plugin installed. Prerequisites: `docker`, `python3`.

OpenSearch requires `vm.max_map_count` to be at least `262144`. If the Docker containers crash on startup, set it with:

```bash
sudo sysctl -w vm.max_map_count=262144
```

To make it persistent across reboots, add `vm.max_map_count=262144` to `/etc/sysctl.conf`.

Build the plugin first, then run the tests:

```bash
bash build.sh 2.19.0
python3 tests/run-tests.py --opensearch-version 2.19.0
```

| Flag | Description |
|---|---|
| `--opensearch-version` | **(required)** OpenSearch version to test against |
| `--plugin-version` | Plugin version (default: `1.0.0`) |
| `--no-teardown` | Keep the cluster running after tests for debugging |

The test suite covers:

- **Test 1 — Basic shard placement**: Creates two datastreams and rolls over repeatedly, verifying `ceil(S/N)` balance after each rollover.
- **Test 2 — New allocation on an imbalanced cluster**: Forces all primaries to one node and all replicas to another, then creates a new datastream and rolls over repeatedly. Verifies that each new shard pair is placed to balance the datastream first, then to balance total shard count per node as a tiebreaker.

## Full Spec

See [SPEC.md](SPEC.md) for the complete design, implementation plan, edge case analysis, and Docker test plan.
