#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")"

IMAGE_NAME="datastream-allocator-builder"
CONTAINER_NAME="datastream-allocator-build"

echo "Building in Docker (rootless)..."
docker build -f Dockerfile.build -t "$IMAGE_NAME" .

echo "Extracting artifacts..."
docker create --name "$CONTAINER_NAME" "$IMAGE_NAME"
mkdir -p build/distributions
docker cp "$CONTAINER_NAME:/build/build/distributions/." build/distributions/
docker cp "$CONTAINER_NAME:/build/build/libs/." build/libs/ 2>/dev/null || true
docker rm "$CONTAINER_NAME"

echo "Done. Artifacts:"
ls -la build/distributions/
