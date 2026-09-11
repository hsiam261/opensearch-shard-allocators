# TODO — Datastream Allocator

## Open Issues

- [ ] **Explain API: distinguish THROTTLE from NO** — `decideAllocateUnassigned()` always returns `DECIDERS_NO` when no node says YES. Should return `DECIDERS_THROTTLED` if any node returned THROTTLE. Same issue in `decideMove()`.

- [ ] **THROTTLE breaks candidate loop early** — In `allocateUnassigned()`, hitting a THROTTLE node breaks the loop without trying remaining nodes. A heavier node might return YES. Consider continuing the search and only falling back to THROTTLED if no YES is found.

- [ ] **`existingAllocationId` dead code** — In `allocateUnassigned()`, the ternary `shard.currentNodeId() != null ? shard.allocationId().getId() : null` is always null for unassigned shards. Simplify to pass `null` directly.

- [ ] **`moveShards()` iterates all routing nodes** — Unlike the rebalance methods which use `getDataNodes()`, `moveShards()` still iterates all nodes including cluster-manager-only ones. Harmless but inconsistent.

- [ ] **Balance overshoot with RELOCATING shards** — RELOCATING shards count on both source and target, causing extra moves in small clusters. By design per spec, matches OpenSearch built-in behavior. Consider optimizing in a future PR.

- [ ] **Optimize rebalance loop** — Sorting all nodes every iteration is O(M * N log N). Could use a priority queue or move multiple shards per iteration before re-sorting.
