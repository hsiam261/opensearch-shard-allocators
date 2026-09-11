# OpenSearch Shard Allocators

A collection of custom shard allocator plugins for [OpenSearch](https://opensearch.org/).

OpenSearch's default `BalancedShardsAllocator` balances shards at the index level, which works well for traditional indices but falls short for workloads like datastreams where each backing index has only one primary and one replica. These plugins provide alternative allocation strategies for specific use cases.

## Allocators

| Allocator | Description |
|---|---|
| [datastream-allocator](allocators/datastream-allocator/) | Balances shards by datastream instead of by index, preventing hotspots in datastream-heavy clusters |

## Repository Structure

```
allocators/
  <allocator-name>/
    README.md       # what it does, how to configure it
    src/            # source code
    tests/          # integration tests
    build.sh        # build script
    SPEC.md         # design spec (if applicable)
```

Each allocator is self-contained with its own build, tests, and documentation. See the individual allocator README for build and installation instructions.

## License

[MIT](LICENSE)
