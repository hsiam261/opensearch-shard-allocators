package io.github.hsiam261.datastreamallocator;

import org.opensearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.ClusterPlugin;
import org.opensearch.plugins.Plugin;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

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
        return List.of(DatastreamShardsAllocator.THRESHOLD_SETTING);
    }
}
