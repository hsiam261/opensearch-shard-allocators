package io.github.hsiam261.datastreamallocator;

import org.opensearch.cluster.metadata.IndexAbstraction;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.RoutingNodes;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.opensearch.cluster.routing.allocation.AllocationDecision;
import org.opensearch.cluster.routing.allocation.MoveDecision;
import org.opensearch.cluster.routing.allocation.NodeAllocationResult;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.cluster.routing.allocation.ShardAllocationDecision;
import org.opensearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.opensearch.cluster.routing.allocation.decider.Decision;
import org.opensearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DatastreamShardsAllocator implements ShardsAllocator {

    // Min difference in datastream shard count (heaviest − lightest node) before
    // balance() moves shards. Higher = more imbalance tolerated, fewer relocations.
    // Dynamic: changeable at runtime via PUT _cluster/settings without restart.
    public static final Setting<Float> THRESHOLD_SETTING = Setting.floatSetting(
        "cluster.routing.allocation.datastream_balance.threshold",
        1.0f,
        0.0f,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    // volatile: the update consumer below writes from the cluster state thread,
    // while allocate() reads from the allocation thread.
    private volatile float threshold;

    public DatastreamShardsAllocator(Settings settings, ClusterSettings clusterSettings) {
        this.threshold = THRESHOLD_SETTING.get(settings);
        // Subscribe to runtime changes — when someone calls PUT _cluster/settings
        // with a new threshold, this lambda fires on every node via cluster state
        // replication and updates our in-memory value.
        clusterSettings.addSettingsUpdateConsumer(THRESHOLD_SETTING, value -> this.threshold = value);
    }

    @Override
    public void allocate(RoutingAllocation allocation) {
        allocateUnassigned(allocation);
        moveShards(allocation);
        balance(allocation);
    }

    private void allocateUnassigned(RoutingAllocation allocation) {
        Metadata metadata = allocation.metadata();
        // Mutating iterator — after each next(), you must call either:
        //   initialize()      → place the shard on a node
        //   removeAndIgnore() → record why it can't be placed (drives retry scheduling)
        RoutingNodes.UnassignedShards.UnassignedIterator iter =
            allocation.routingNodes().unassigned().iterator();

        while (iter.hasNext()) {
            ShardRouting shard = iter.next();
            String datastream = resolveDatastream(shard.getIndexName(), metadata);
            List<RoutingNode> candidates = sortedCandidates(allocation, datastream, metadata);

            boolean assigned = false;
            boolean throttled = false;
            for (RoutingNode node : candidates) {
                Decision decision = allocation.deciders().canAllocate(shard, node, allocation);
                if (decision.type() == Decision.Type.YES) {
                    String existingAllocationId = shard.currentNodeId() != null
                        ? shard.allocationId().getId() : null;
                    iter.initialize(node.nodeId(), existingAllocationId, expectedShardSize(shard, allocation), allocation.changes());
                    assigned = true;
                    break;
                }
                if (decision.type() == Decision.Type.THROTTLE) {
                    throttled = true;
                    break;
                }
            }
            if (!assigned) {
                iter.removeAndIgnore(
                    throttled
                        ? UnassignedInfo.AllocationStatus.DECIDERS_THROTTLED
                        : UnassignedInfo.AllocationStatus.DECIDERS_NO,
                    allocation.changes()
                );
            }
        }
    }

    // Relocate STARTED shards that deciders say can no longer remain on their node.
    // If no target accepts, the shard stays put — next reroute will retry.
    private void moveShards(RoutingAllocation allocation) {
        Metadata metadata = allocation.metadata();

        for (RoutingNode node : allocation.routingNodes()) {
            List<ShardRouting> started = new ArrayList<>();
            for (ShardRouting shard : node) {
                if (shard.started()) {
                    started.add(shard);
                }
            }

            for (ShardRouting shard : started) {
                Decision remainDecision = allocation.deciders().canRemain(shard, node, allocation);
                if (remainDecision.type() == Decision.Type.NO) {
                    String datastream = resolveDatastream(shard.getIndexName(), metadata);

                    List<RoutingNode> candidates = sortedCandidates(allocation, datastream, metadata);

                    for (RoutingNode target : candidates) {
                        if (target.nodeId().equals(node.nodeId())) continue;
                        Decision allocateDecision = allocation.deciders().canAllocate(shard, target, allocation);
                        if (allocateDecision.type() == Decision.Type.YES) {
                            // relocateShard(shard, nodeId, expectedSize, changes):
                            //   expectedSize — bytes, for disk threshold checks (-1 = unknown)
                            //   changes      — observer that records the routing table mutation
                            // Shard becomes RELOCATING on source + INITIALIZING on target.
                            allocation.routingNodes().relocateShard(
                                shard, target.nodeId(),
                                allocation.clusterInfo().getShardSize(shard, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE),
                                allocation.changes()
                            );
                            break;
                        }
                    }
                }
            }
        }
    }

    private void balance(RoutingAllocation allocation) {
        if (allocation.deciders().canRebalance(allocation).type() != Decision.Type.YES) {
            return;
        }

        Metadata metadata = allocation.metadata();

        Set<String> datastreams = new HashSet<>();
        for (Map.Entry<String, IndexAbstraction> entry : metadata.getIndicesLookup().entrySet()) {
            if (entry.getValue().getType() == IndexAbstraction.Type.DATA_STREAM) {
                datastreams.add(entry.getKey());
            }
        }

        for (String datastream : datastreams) {
            rebalanceDatastream(datastream, allocation, metadata);
        }

        rebalanceNonDatastream(allocation, metadata);
    }

    private void rebalanceDatastream(String datastream, RoutingAllocation allocation, Metadata metadata) {
        boolean moved = true;
        while (moved) {
            moved = false;

            List<RoutingNode> nodes = new ArrayList<>();
            for (RoutingNode node : allocation.routingNodes()) {
                nodes.add(node);
            }
            nodes.sort(Comparator.comparingInt(n -> countDatastreamShards(n, datastream, metadata)));

            RoutingNode lightest = nodes.get(0);
            RoutingNode heaviest = nodes.get(nodes.size() - 1);
            int lightCount = countDatastreamShards(lightest, datastream, metadata);
            int heavyCount = countDatastreamShards(heaviest, datastream, metadata);

            if (heavyCount - lightCount <= threshold) break;

            List<ShardRouting> heaviestShards = new ArrayList<>();
            for (ShardRouting s : heaviest) {
                heaviestShards.add(s);
            }

            for (ShardRouting shard : heaviestShards) {
                if (!shard.started()) continue;
                String ds = resolveDatastream(shard.getIndexName(), metadata);
                if (!datastream.equals(ds)) continue;

                Decision rebalanceDecision = allocation.deciders().canRebalance(shard, allocation);
                if (rebalanceDecision.type() != Decision.Type.YES) continue;

                // Try lightest first, then walk up
                for (int i = 0; i < nodes.size() - 1; i++) {
                    RoutingNode target = nodes.get(i);
                    int targetCount = countDatastreamShards(target, datastream, metadata);
                    if (heavyCount - targetCount <= threshold) break;

                    Decision allocateDecision = allocation.deciders().canAllocate(shard, target, allocation);
                    if (allocateDecision.type() == Decision.Type.YES) {
                        allocation.routingNodes().relocateShard(
                            shard, target.nodeId(), 0L, allocation.changes()
                        );
                        moved = true;
                        break;
                    }
                }
                if (moved) break;
            }
        }
    }

    private void rebalanceNonDatastream(RoutingAllocation allocation, Metadata metadata) {
        boolean moved = true;
        while (moved) {
            moved = false;

            List<RoutingNode> nodes = new ArrayList<>();
            for (RoutingNode node : allocation.routingNodes()) {
                nodes.add(node);
            }
            nodes.sort(Comparator.comparingInt(RoutingNode::size));

            RoutingNode lightest = nodes.get(0);
            RoutingNode heaviest = nodes.get(nodes.size() - 1);

            if (heaviest.size() - lightest.size() <= threshold) break;

            List<ShardRouting> heaviestShards = new ArrayList<>();
            for (ShardRouting s : heaviest) {
                heaviestShards.add(s);
            }

            for (ShardRouting shard : heaviestShards) {
                if (!shard.started()) continue;
                if (resolveDatastream(shard.getIndexName(), metadata) != null) continue;

                Decision rebalanceDecision = allocation.deciders().canRebalance(shard, allocation);
                if (rebalanceDecision.type() != Decision.Type.YES) continue;

                for (int i = 0; i < nodes.size() - 1; i++) {
                    RoutingNode target = nodes.get(i);
                    if (heaviest.size() - target.size() <= threshold) break;

                    Decision allocateDecision = allocation.deciders().canAllocate(shard, target, allocation);
                    if (allocateDecision.type() == Decision.Type.YES) {
                        allocation.routingNodes().relocateShard(
                            shard, target.nodeId(), 0L, allocation.changes()
                        );
                        moved = true;
                        break;
                    }
                }
                if (moved) break;
            }
        }
    }

    @Override
    public ShardAllocationDecision decideShardAllocation(ShardRouting shard, RoutingAllocation allocation) {
        if (shard.unassigned()) {
            return new ShardAllocationDecision(
                decideAllocateUnassigned(shard, allocation),
                MoveDecision.NOT_TAKEN
            );
        }
        if (shard.started()) {
            return new ShardAllocationDecision(
                AllocateUnassignedDecision.NOT_TAKEN,
                decideMove(shard, allocation)
            );
        }
        return ShardAllocationDecision.NOT_TAKEN;
    }

    private AllocateUnassignedDecision decideAllocateUnassigned(ShardRouting shard, RoutingAllocation allocation) {
        Metadata metadata = allocation.metadata();
        String datastream = resolveDatastream(shard.getIndexName(), metadata);
        List<RoutingNode> candidates = sortedCandidates(allocation, datastream, metadata);

        List<NodeAllocationResult> nodeDecisions = new ArrayList<>();
        RoutingNode bestNode = null;
        int rank = 0;

        for (RoutingNode node : candidates) {
            Decision decision = allocation.deciders().canAllocate(shard, node, allocation);
            nodeDecisions.add(new NodeAllocationResult(node.node(), decision, ++rank));

            if (bestNode == null && decision.type() == Decision.Type.YES) {
                bestNode = node;
            }
        }

        if (bestNode != null) {
            return AllocateUnassignedDecision.yes(bestNode.node(), null, nodeDecisions, false);
        }
        return AllocateUnassignedDecision.no(
            org.opensearch.cluster.routing.UnassignedInfo.AllocationStatus.DECIDERS_NO,
            nodeDecisions
        );
    }

    private MoveDecision decideMove(ShardRouting shard, RoutingAllocation allocation) {
        Metadata metadata = allocation.metadata();
        RoutingNode currentNode = allocation.routingNodes().node(shard.currentNodeId());
        if (currentNode == null) return MoveDecision.NOT_TAKEN;

        Decision canRemain = allocation.deciders().canRemain(shard, currentNode, allocation);
        if (canRemain.type() == Decision.Type.YES) {
            return MoveDecision.stay(canRemain);
        }

        String datastream = resolveDatastream(shard.getIndexName(), metadata);
        List<RoutingNode> candidates = sortedCandidates(allocation, datastream, metadata);
        List<NodeAllocationResult> nodeDecisions = new ArrayList<>();
        int rank = 0;

        for (RoutingNode target : candidates) {
            if (target.nodeId().equals(currentNode.nodeId())) continue;
            Decision allocateDecision = allocation.deciders().canAllocate(shard, target, allocation);
            nodeDecisions.add(new NodeAllocationResult(target.node(), allocateDecision, ++rank));
        }

        return MoveDecision.cannotRemain(canRemain, AllocationDecision.NO, null, nodeDecisions);
    }

    // --- helpers ---

    private String resolveDatastream(String indexName, Metadata metadata) {
        IndexAbstraction abs = metadata.getIndicesLookup().get(indexName);
        if (abs == null) return null;
        IndexAbstraction.DataStream parent = abs.getParentDataStream();
        if (parent == null) return null;
        return parent.getName();
    }

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

    private long expectedShardSize(ShardRouting shard, RoutingAllocation allocation) {
        return DiskThresholdDecider.getExpectedShardSize(
            shard,
            ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE,
            allocation.clusterInfo(),
            allocation.snapshotShardSizeInfo(),
            allocation.metadata(),
            allocation.routingTable()
        );
    }

    private List<RoutingNode> sortedCandidates(RoutingAllocation allocation, String datastream, Metadata metadata) {
        List<RoutingNode> candidates = new ArrayList<>();
        for (RoutingNode node : allocation.routingNodes()) {
            candidates.add(node);
        }

        if (datastream != null) {
            candidates.sort(Comparator.comparingInt(n -> countDatastreamShards(n, datastream, metadata)));
        } else {
            candidates.sort(Comparator.comparingInt(RoutingNode::size));
        }
        return candidates;
    }
}
