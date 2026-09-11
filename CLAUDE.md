# CLAUDE.md

## Project Overview

A collection of custom shard allocator plugins for OpenSearch.

## Repository Structure

```
allocators/
  <allocator-name>/
    README.md       # allocator-specific documentation
    src/            # source code
    tests/          # tests
```

Each allocator lives in its own directory under `allocators/` and is self-contained with its own README, source, and tests.

## Working with Allocators

- When adding a new allocator, create a new directory under `allocators/` following the existing structure.
- Each allocator should have its own README explaining what it does, how to build it, and how to install it as an OpenSearch plugin.
- Tests for an allocator live alongside it in its `tests/` directory.
