#!/bin/bash
# Build the single corrected linux/amd64 image and push one immutable experiment tag.

set -euo pipefail

DOCKERHUB_USER="${1:?Usage: $0 DOCKERHUB_USER [TAG]}"
TAG="${2:-sc26}"
IMAGE="$DOCKERHUB_USER/canopie-storage-benchmark:$TAG"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$ROOT"
docker buildx build \
    --platform linux/amd64 \
    --tag "$IMAGE" \
    --file docker/Dockerfile \
    --push \
    .

echo "$IMAGE"
